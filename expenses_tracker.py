import asyncio
from dataclasses import dataclass
import datetime as dt
from decimal import Decimal, InvalidOperation
import logging
from typing import Optional, Type

import pandas as pd
from telethon.tl.patched import Message
from unidecode import unidecode

from tel2gsheet import (
    IncomingMessage, Tracker, TelegramConnection, GSheetConnection,
    load_chat_id, load_sheet_id
)


logger = logging.getLogger(__name__)
TEXT_HEADER = "# MENSAGEM AUTOMATICA #\n\n"
HELP_MESSAGE = (
    "As mensagens devem ter o seguinte formato:\n"
    "PRECO - CONTRAPRATE - DESCRICAO - CONTA - DATA (OPCIONAL)\n"
    "PRECO - Valor numerico com ponto como separador decimal, e.g., 70, 35.2\n"
    "CONTRAPRATE - Contraparte da despesa, e.g., Uber, Dia Santo Antonio\n"
    "DESCRICAO - Descrição da despesa, e.g., Uber p/ shopping\n"
    "CONTA - Conta usada para o pagamento, e.g., BB Corrente, XP Credito\n"
    "DATA - Data da transação. Se não for especificado, assume-se a data da "
    "mensagem como data da despesa\n\n"
    "Ex:\n"
    "19.94 - Uber - Uber p/ Hospital - NuBank Credito - 2023-12-28\n"
    "20 - Loterica - Aposta Mega Virada - Dinheiro"
)
TRACKER_NAME = "expenses"


def normalize_text(text: str) -> str:
    return unidecode(text.lower().strip())


@dataclass
class ExpenseMessage(IncomingMessage):
    """TODO: add docstring"""
    id: int
    date: dt.date
    chat_id: int
    price: Decimal
    counterparty: str
    description: str
    account: str

    def __str__(self):
        return (f"{self.price} - {self.counterparty} - "
                f"{self.description} - {self.account} - "
                f"{self.date.strftime('%Y-%m-%d')}")

    @classmethod
    def parse(cls, msg: Message, valid_accounts = set[str]):
        items = [i.strip() for i in msg.text.split("-")]
        price = Decimal(items[0].replace(",", "."))
        cp = items[1]
        desc = items[2]
        matching_accs = [a for a in valid_accounts 
                         if normalize_text(a) == normalize_text(items[3])]
        acc = matching_accs[0]
        date = (msg.date.date() if len(items) < 5 else
                dt.datetime.strptime(items[4], "%d/%m/%Y"))
        return cls(msg.id, date, msg.chat_id, price, cp, desc, acc)


@dataclass
class ExpensesTracker(Tracker):
    telegram: TelegramConnection
    gsheet: GSheetConnection
    chat_id: int
    spreadsheet_id: int
    name: str = TRACKER_NAME
    month_names = {
        1: "jan", 2: "fev", 3: "mar", 4: "abr", 5: "mai", 6: "jun", 7: "jul",
        8: "ago", 9: "set", 10: "out", 11: "nov", 12: "dez"
    }
    help_message = HELP_MESSAGE
    text_header = TEXT_HEADER

    def __post_init__(self):
        self.to_upload: list[ExpenseMessage] = []
        self.failed_to_parse: list[Message] = []
        self.asked_for_help: list[Message] = []

    @classmethod
    def from_yaml(cls, file_path: str = "settings.yaml"):
        tel = TelegramConnection.from_yaml(file_path)
        gsheet = GSheetConnection()
        chat_id = load_chat_id(TRACKER_NAME, file_path)
        sheet_id = load_sheet_id(TRACKER_NAME, file_path)
        return cls(tel, gsheet, chat_id, sheet_id)

    def fetch_telegram_messages(self) -> list[Message]:
        return asyncio.get_event_loop().run_until_complete(
            self.telegram.fetch_msgs(self.chat_id)
        )

    def send_telegram_message(
            self, chat_id: int, txt: str, reply_to: Optional[int]
    ):
        asyncio.get_event_loop().run_until_complete(
            self.telegram.send_msg(chat_id, txt, reply_to)
        )

    def process_received_messages(self):
        msgs = [m for m in self.fetch_telegram_messages()
                if m.id not in self.processed_msgs]
        if not msgs:
            return ([], [])

        accounts = set(self.accounts_df[self.accounts_df["Tipo"].isin(
            ["Asset", "Liability"]
        )]["Conta"].values.tolist())

        help_wanted: list[int] = []
        answered: set[int] = set()
        for msg in msgs:
            if msg.is_reply:
                answered.add(msg.reply_to.reply_to_msg_id)
            elif msg.text is None or msg.text.startswith("#"):
                continue
            if msg.text.strip() == "?":
                help_wanted.append(msg)
            else:
                try:
                    self.to_upload.append(ExpenseMessage.parse(msg, accounts))
                except (IndexError, InvalidOperation, IndexError) as e:
                    logging.error(e)
                    self.failed_to_parse.append(msg)

        self.to_upload.reverse()
        self.failed_to_parse.reverse()
        self.asked_for_help.extend(
            [msg for msg in help_wanted if msg.id not in answered]
        )
        return

    def fetch_entries(self) -> pd.DataFrame:
        sheet_name = "Entradas"
        sheet_range = "A:F"
        df = self.gsheet.read(self.spreadsheet_id, sheet_name, sheet_range)
        return df

    def fetch_accounts(self) -> pd.DataFrame:
        sheet_name = "Contas"
        sheet_range = "B2:C999"
        df = self.gsheet.read(self.spreadsheet_id, sheet_name, sheet_range)
        return df

    def fetch_processed_messages(self) -> set[int]:
        sheet_name = "Telegram"
        sheet_range = "A:B"
        df = self.gsheet.read(self.spreadsheet_id, sheet_name, sheet_range)
        return {int(i) for i in df["MsgId"].values if i}

    def fetch_gsheet_state(self):
        self.entries_df = self.fetch_entries()
        self.accounts_df = self.fetch_accounts()
        self.processed_msgs = self.fetch_processed_messages()

    def clean_local_state(self):
        self.to_upload = []
        self.failed_to_parse = []
        self.asked_for_help = []
        del self.entries_df
        del self.accounts_df
        del self.processed_msgs

    def upload_processed_messages(self):
        sheet_name = "Telegram"
        sheet_range = "A:B"
        data = (
            [[msg.id, "SUCCESS"] for msg in self.to_upload] +
            [[msg.id, "FAILURE"] for msg in self.failed_to_parse] +
            [[msg.id, "ASKED_FOR_HELP"] for msg in self.asked_for_help]
        )
        self.gsheet.write(self.spreadsheet_id, sheet_name, sheet_range, data)

    def upload_entries(self):

        def dt_fmt(d: dt.date) -> str:
            return f"{d.day:02}-{self.month_names[d.month]}.-{d.year}"

        def dec_fmt(d: Decimal) -> str:
            return str(d).replace(".", ",")

        sheet_name = "Entradas"
        sheet_range = "A:F"
        data = []
        for m in self.to_upload:
            data.extend([
                [
                    dt_fmt(m.date),
                    m.counterparty,
                    m.description,
                    a,
                    dec_fmt(p),
                    m.id
                ] for a, p in [("Despesa", m.price), (m.account, -m.price)]
            ])
        self.gsheet.write(self.spreadsheet_id, sheet_name, sheet_range, data)

    def update_gsheet_state(self):
        self.upload_entries()
        self.upload_processed_messages()

    def send_feedback_messages(self):
        if not (self.to_upload or self.failed_to_parse):
            return

        ok_msgs = "\n\n".join([f"\t-> {str(m)}" for m in self.to_upload])
        nok_msgs = "\n\n".join(
            set(f"\t-> {m.text}" for m in self.failed_to_parse)
        )
        success_text = "As seguintes mensagens foram registradas:"
        failure_text = ("As seguintes mensagens não foram registradas por "
                        "erro de formatação:")

        if ok_msgs:
            cur_year = dt.datetime.now().year
            cur_month = dt.datetime.now().month
            date_str = dt.datetime.now().strftime("%m/%Y")
            spent = self.get_monthly_expenses(cur_year, cur_month)
            spent_text = (f"No mês {date_str} houveram {len(spent)} "
                          f"despesas totalizando R$ {sum(spent)} gastos")
        else:
            spent_text = ""

        text = (
            self.text_header +
            (f"{success_text}\n\n{ok_msgs}\n\n\n" if ok_msgs else "") +
            (f"{failure_text}\n\n{nok_msgs}\n\n\n" if nok_msgs else "") +
            spent_text +
            "\n\n" +
            "Em caso de dúvidas, envie uma mensagem contendo o caractere '?'"
        )
        self.send_telegram_message(self.chat_id, text, None)
        if self.asked_for_help:

            accounts = set(self.accounts_df[self.accounts_df["Tipo"].isin(
                ["Asset", "Liability"]
            )]["Conta"].values.tolist())

            valid_acc_text = f"\n\nContas válidas: {', '.join(accounts)}"
            help_msg = self.text_header + self.help_message + valid_acc_text
            self.send_telegram_message(
                self.chat_id, help_msg, max(self.asked_for_help)
            )

    def get_monthly_expenses(self, year: int, month: int) -> list[Decimal]:
        self.fetch_gsheet_state()
        df = self.entries_df.copy()
        month_from_name = {v: k for k, v in self.month_names.items()}

        def _replace(d: str) -> str:
            for name in month_from_name:
                d = d.replace(name, f"{month_from_name[name]:02}")
            return d

        df["Data"] = df["Data"].apply(lambda d: _replace(d))
        df["RefDate"] = df["Data"].apply(
            lambda d: dt.datetime.strptime(d, "%d-%m.-%Y").date()
        )
        df["Month"] = df["RefDate"].apply(lambda d: d.month)
        df["Year"] = df["RefDate"].apply(lambda d: d.year)

        df["Values"] = df[
            (df["Conta"] == "Despesa") &
            (df["Month"] == month) &
            (df["Year"] == year)
        ]["Valor (R$)"].apply(
            lambda v: Decimal(v.replace("R$ ", "").replace(",", "."))
        )
        return df[~df["Values"].isna()]["Values"].values.tolist()

    def run(self):
        self.fetch_gsheet_state()
        self.process_received_messages()
        self.update_gsheet_state()
        self.send_feedback_messages()
        self.clean_local_state()


def load_and_run_trackers(trackers: list[Type[Tracker]],
                          yaml_file_path: str = "settings.yaml"):
    t_objs = [t.from_yaml(yaml_file_path) for t in trackers]
    for tracker in t_objs:
        tracker.run()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    load_and_run_trackers([ExpensesTracker])