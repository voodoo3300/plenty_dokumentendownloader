import io
import sys
import json
import os
import zipfile
from datetime import datetime
from PyQt6.QtWidgets import (
    QApplication, QWidget, QVBoxLayout, QHBoxLayout, QLabel, QLineEdit,
    QPushButton, QTabWidget, QCheckBox, QListWidget, QFileDialog,
    QDateTimeEdit, QMessageBox, QScrollArea, QGroupBox, QGridLayout,
    QDateEdit, QRadioButton, QButtonGroup, QSpinBox
)
from PyQt6.QtCore import Qt, QDateTime, QDate, QThread, pyqtSignal
from dotenv import load_dotenv
from plentymarkets_client import PlentymarketsClient

CONFIG_PATH = "plenty_config.json"
DOCUMENT_TYPES = [
    "receipt", "zReport", "tillCount", "posCouponReceipt", "posInvoice",
    "posInvoiceCancellation", "cancellation", "invoiceExternal", "invoice",
    "deliveryNote", "creditNote", "creditNoteExternal", "orderConfirmation",
    "offer", "dunningLetter", "reversalDunningLetter", "returnNote",
    "successConfirmation", "correction", "reorder", "uploaded"
]

class DocumentDownloadWorker(QThread):
    log_signal = pyqtSignal(str)

    def __init__(self, base_url, username, password, types, created_at_from, created_at_to, batch_size, target_dir, filename_prefix):
        super().__init__()
        self.base_url = base_url
        self.username = username
        self.password = password
        self.types = types
        self.created_at_from = created_at_from
        self.created_at_to = created_at_to
        self.batch_size = batch_size
        self.target_dir = target_dir or os.getcwd()
        self.filename_prefix = filename_prefix or "PlentyDocuments"

    def __is_zip_empty(self, zip_bytes: bytes) -> bool:
        try:
            with zipfile.ZipFile(io.BytesIO(zip_bytes)) as zip_file:
                return len(zip_file.namelist()) == 0
        except zipfile.BadZipFile:
            return True  # oder False, je nach gewünschtem Verhalten

    def run(self):
        credentials = {"username": self.username, "password": self.password}
        plenty = None

        try:
            plenty = PlentymarketsClient(self.base_url, credentials)
        except Exception as e:
            self.log_signal.emit(f"Kann keine Verbindung zu Plenty aufbauen - Zugangsdaten korrekt eingegeben? – {str(e)}")

        if plenty:
            for doc_type in self.types:
                self.log_signal.emit(f"{doc_type}: Abruf gestartet ...")
                page = 1

                stop_condition = False






                while not stop_condition:
                    try:
                        data = plenty.get_documents_by_type(
                            doc_type,
                            createdAtFrom=self.created_at_from,
                            createdAtTo=self.created_at_to,
                            batchSize=self.batch_size,
                            page = page
                        )

                        filename = os.path.join(self.target_dir, f"{self.filename_prefix}_{doc_type}-{page}.zip")
                        if data and not self.__is_zip_empty(data):
                            with open(filename, "wb") as f:
                                f.write(data)
                            self.log_signal.emit(f"{doc_type}: Batch {page} geladen!")
                            page += 1
                            stop_condition = False
                        else:
                            stop_condition = True
                            self.log_signal.emit(f"{doc_type}: Keine Daten mehr - Fertig")


                    except Exception as e:
                        self.log_signal.emit(f"{doc_type}: Fehler – {str(e)}")
                        self.log_signal.emit(f"ABBRUCH!")
                        stop_condition = True


def load_config():
    if os.path.exists(CONFIG_PATH):
        with open(CONFIG_PATH, "r") as f:
            return json.load(f)
    return {}

def save_config(config):
    with open(CONFIG_PATH, "w") as f:
        json.dump(config, f, indent=4)

class PlentyDownloader(QWidget):
    def __init__(self):
        super().__init__()
        self.setWindowTitle("Plenty Dokumente herunterladen")
        self.resize(800, 600)

        load_dotenv()
        self.config = load_config()
        self.init_ui()

    def init_ui(self):
        layout = QVBoxLayout(self)

        tabs = QTabWidget()
        tabs.addTab(self.create_download_tab(), "Dokumente")

        settings_button = QPushButton("Zugangsdaten ...")
        settings_button.clicked.connect(self.open_settings_dialog)
        layout.addWidget(settings_button)

        layout.addWidget(tabs)

    def open_settings_dialog(self):
        dialog = QWidget()
        dialog.setWindowTitle("Zugangsdaten")
        layout = QVBoxLayout(dialog)

        url_input = QLineEdit(self.config.get("base_url", ""))
        user_input = QLineEdit(self.config.get("username", ""))
        pass_input = QLineEdit(self.config.get("password", ""))
        pass_input.setEchoMode(QLineEdit.EchoMode.Password)

        layout.addWidget(QLabel("System URL:"))
        layout.addWidget(url_input)
        layout.addWidget(QLabel("Benutzername:"))
        layout.addWidget(user_input)
        layout.addWidget(QLabel("Passwort:"))
        layout.addWidget(pass_input)
        layout.addWidget(QLabel("Hinweis: Die Zugangsdaten werden im Klartext gespeichert."))

        save_btn = QPushButton("Speichern")
        def save():
            self.config = {
                "base_url": url_input.text().strip(),
                "username": user_input.text().strip(),
                "password": pass_input.text().strip()
            }
            save_config(self.config)
            QMessageBox.information(self, "Gespeichert", "Zugangsdaten gespeichert.")
            dialog.close()
        save_btn.clicked.connect(save)
        layout.addWidget(save_btn)

        dialog.setLayout(layout)
        dialog.setFixedSize(400, 300)
        dialog.show()

    def create_download_tab(self):
        def generate_default_prefix():
            return "PlentyDocuments_" + datetime.now().strftime("%Y_%m_%d_%H%M%S")
        tab = QWidget()
        layout = QVBoxLayout(tab)

        self.checkbox_group = QGroupBox("Dokumenttypen")
        grid_layout = QGridLayout()
        self.checkboxes = {}
        for i, doc_type in enumerate(DOCUMENT_TYPES):
            cb = QCheckBox(doc_type)
            cb.setChecked(True)
            self.checkboxes[doc_type] = cb
            row = i // 2
            col = i % 2
            grid_layout.addWidget(cb, row, col)
        self.checkbox_group.setLayout(grid_layout)
        layout.addWidget(self.checkbox_group)

        # Radiobuttons zur Auswahl der Datumseingrenzung
        date_option_layout = QHBoxLayout()
        self.date_all_radio = QRadioButton("Alle Daten")
        self.date_all_radio.setChecked(True)
        self.date_range_radio = QRadioButton("Datumsbereich eingrenzen")
        # self.date_range_radio.setChecked(True)
        date_option_layout.addWidget(self.date_all_radio)
        date_option_layout.addWidget(self.date_range_radio)
        layout.addLayout(date_option_layout)

        self.date_button_group = QButtonGroup()
        self.date_button_group.addButton(self.date_all_radio)
        self.date_button_group.addButton(self.date_range_radio)
        self.date_button_group.buttonToggled.connect(self.toggle_date_inputs)

        # Datumsfelder
        date_layout = QHBoxLayout()
        self.date_from = QDateEdit(QDate.currentDate())
        self.date_from.setCalendarPopup(True)
        self.date_from.setDisplayFormat("yyyy-MM-dd")

        self.date_to = QDateEdit(QDate.currentDate())
        self.date_to.setCalendarPopup(True)
        self.date_to.setDisplayFormat("yyyy-MM-dd")

        date_layout.addWidget(QLabel("Von:"))
        date_layout.addWidget(self.date_from)
        date_layout.addWidget(QLabel("Bis:"))
        date_layout.addWidget(self.date_to)
        layout.addLayout(date_layout)

        # Speicherort
        path_layout = QHBoxLayout()
        path_layout.addWidget(QLabel("Speicherort:"))
        self.download_path_input = QLineEdit()
        self.download_path_input.setReadOnly(True)
        browse_btn = QPushButton("...")
        browse_btn.clicked.connect(self.choose_download_path)
        path_layout.addWidget(self.download_path_input)
        path_layout.addWidget(browse_btn)
        layout.addLayout(path_layout)

        # Dateipräfix
        prefix_layout = QHBoxLayout()
        prefix_layout.addWidget(QLabel("Dateipräfix:"))
        self.filename_prefix = QLineEdit(generate_default_prefix())
        prefix_layout.addWidget(self.filename_prefix)
        layout.addLayout(prefix_layout)

        # Batchgröße
        batch_layout = QHBoxLayout()
        batch_layout.addWidget(QLabel("Batchgröße:"))
        self.batch_size = QSpinBox()
        self.batch_size.setRange(50, 6000)
        self.batch_size.setSingleStep(50)
        self.batch_size.setValue(1000)
        batch_layout.addWidget(self.batch_size)
        layout.addLayout(batch_layout)

        self.toggle_date_inputs()  # initial setzen
        self.date_all_radio.setChecked(True)

        self.download_button = QPushButton("Download starten")
        self.download_button.clicked.connect(self.download_documents)
        layout.addWidget(self.download_button)

        self.log_list = QListWidget()
        layout.addWidget(self.log_list)

        return tab

    def toggle_date_inputs(self):
        enabled = self.date_range_radio.isChecked()
        self.date_from.setEnabled(enabled)
        self.date_to.setEnabled(enabled)

    def log(self, message):
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        self.log_list.addItem(f"[{timestamp}] {message}")
        self.log_list.scrollToBottom()

    def save_settings(self):
        self.config = {
            "base_url": self.url_input.text().strip(),
            "username": self.user_input.text().strip(),
            "password": self.pass_input.text().strip()
        }
        save_config(self.config)
        QMessageBox.information(self, "Gespeichert", "Zugangsdaten gespeichert.")

    def choose_download_path(self):
        path = QFileDialog.getExistingDirectory(self, "Zielordner wählen")
        if path:
            self.download_path_input.setText(path)

    def download_documents(self):
        self.checkbox_group.hide()
        self.date_all_radio.hide()
        self.date_range_radio.hide()
        self.date_from.hide()
        self.date_to.hide()
        self.download_button.hide()
        self.batch_size.hide()
        for widget in self.findChildren(QLabel):
            widget.hide()
        for layout in self.findChildren(QHBoxLayout):
            for i in range(layout.count()):
                item = layout.itemAt(i).widget()
                if item:
                    item.hide()

        selected_types = [k for k, cb in self.checkboxes.items() if cb.isChecked()]

        if self.date_range_radio.isChecked():
            created_at_from = self.date_from.date().toString("yyyy-MM-ddT00:00:00+00:00")
            created_at_to = self.date_to.date().toString("yyyy-MM-ddT23:59:59+00:00")
        else:
            created_at_from = None
            created_at_to = None

        batch_size = self.batch_size.value()

        base_url = self.config.get("base_url") or os.getenv("PLENTY_SYSTEM_URL")
        username = self.config.get("username") or os.getenv("PLENTY_API_USER")
        password = self.config.get("password") or os.getenv("PLENTY_API_PASSWORD")

        self.log(f"Starte Download: Typen={selected_types}, Von={created_at_from}, Bis={created_at_to}, Batchgröße={batch_size}")

        self.worker = DocumentDownloadWorker(base_url, username, password, selected_types, created_at_from, created_at_to, batch_size, self.download_path_input.text(), self.filename_prefix.text())
        self.worker.log_signal.connect(self.log)
        self.worker.start()

if __name__ == '__main__':
    app = QApplication(sys.argv)
    window = PlentyDownloader()
    window.show()
    sys.exit(app.exec())
