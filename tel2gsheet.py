from abc import ABC, abstractmethod
import asyncio
from dataclasses import dataclass, field
import datetime as dt
from decimal import Decimal, InvalidOperation
import logging
import os.path
import time
from typing import Optional, Type

from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
import pandas as pd
from telethon import TelegramClient
from telethon.tl.patched import Message
from unidecode import unidecode
import yaml


logger = logging.getLogger(__name__)
GOOGLE_SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]
EXPENSES_TRACKER_HELP_MESSAGE = (
    "# MENSAGEM AUTOMATICA #"
    "\nMensagem de ajuda em construção"
)
EXPENSES_TRACKER_NAME = "expenses"


def load_chat_id(chat: str, yaml_file_path: str = "settings.yaml") -> int:
    logger.info("Fetching chat id for '%s' from '%s'", chat, yaml_file_path)
    with open(yaml_file_path, "r", encoding="UTF-8") as f:
        cfg = yaml.safe_load(f)
        chat_id = cfg["telegram"]["chats"][chat]["id"]
    return chat_id


def load_sheet_id(sheet: str, yaml_file_path: str = "settings.yaml") -> str:
    with open(yaml_file_path, "r", encoding="UTF-8") as f:
        cfg = yaml.safe_load(f)
        sheet_id = cfg["google"]["sheets"][sheet]["id"]
    return sheet_id


def normalize_text(text: str) -> str:
    return unidecode(text.lower().strip())


@dataclass
class TelegramConnection:
    name: str
    id: int
    hash: str

    def __post_init__(self):
        self.client = TelegramClient(
            self.name, self.id, self.hash
        )

    @classmethod
    def from_yaml(cls, file_path: str = "settings.yaml"):
        logger.info("Loading Telegram config from '%s'", file_path)
        with open(file_path, "r", encoding="UTF-8") as f:
            cfg = yaml.safe_load(f)
            api_id = cfg["telegram"]["client"]["id"]
            api_hash = cfg["telegram"]["client"]["hash"]
            name = cfg["telegram"]["client"]["name"]
            logger.debug("name: %s; id: %s, hash: %s",
                         name, api_id, api_hash)
            return cls(name, api_id, api_hash)

    async def send_msg(self, chat_id: int, txt: str, reply_to: Optional[int]):
        logger.info("Sending to chat %d: %s", chat_id, txt)
        async with self.client:
            await self.client.send_message(
                entity=chat_id,
                message=txt,
                reply_to=reply_to
            )

        """TODO: add docstring"""
    async def fetch_msgs(self, chat_id: int) -> list[Message]:
        client = self.client
        async with client:
            msgs = list(await client.get_messages(chat_id, limit=100))
        return msgs


@dataclass
class Sheet:
    id: str

class IncomingMessage(ABC):
    id: int
    date: dt.date
    chat_id: int

    @abstractmethod
    def __str__(self):
        pass

    @classmethod
    @abstractmethod
    def parse(cls, msg: Message):
        pass


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
class GSheetConnection:
    credentials_file: str = "google-credentials.json"
    token_file: str = "google-token.json"
    scopes: list[str] = field(default_factory=lambda: GOOGLE_SCOPES)

    def __post_init__(self):
        self.sheets_svc = self._build_svc().spreadsheets()

    def _build_svc(self):
        token_file = self.token_file
        creds_file = self.credentials_file
        creds: Optional[Credentials] = None
        if os.path.exists(token_file):
            creds = Credentials.from_authorized_user_file(token_file, self.scopes)
            if not creds or not creds.valid:
                if creds and creds.expired and creds.refresh_token:
                    creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(
                creds_file, self.scopes)
            creds = flow.run_local_server(port=0)
            with open(token_file, "w") as token:
                token.write(creds.to_json())
        svc = build("sheets", "v4", credentials=creds)
        return svc

    def read(self, gsheet_id: str, sheet: str, sheet_range: str,
             includes_columns: bool = True) -> pd.DataFrame:
        result = self.sheets_svc.values().get(
            spreadsheetId=gsheet_id,
            range=f"{sheet}!{sheet_range}"
        ).execute()
        values = result.get("values", [])
        df = pd.DataFrame(values)
        if includes_columns and not df.empty:
            df.columns = df.iloc[0]
            df = df.drop(df.index[0])
        return df

    def write(self, gsheet_id: str, sheet: str, sheet_range: str,
              data: list[list]):
        self.sheets_svc.values().append(
            spreadsheetId=gsheet_id,
            range=f"{sheet}!{sheet_range}",
            valueInputOption="USER_ENTERED",
            body={"values": data}
        ).execute()


class Tracker(ABC):
    telegram: TelegramConnection
    gsheet: GSheetConnection
    name: str

    @abstractmethod
    def fetch_telegram_messages(self):
        pass

    @abstractmethod
    def fetch_gsheet_state(self):
        pass

    @abstractmethod
    def process_received_messages(self):
        pass

    @abstractmethod
    def update_gsheet_state(self):
        pass

    @abstractmethod
    def send_feedback_messages(self):
        pass

    @abstractmethod
    def clean_local_state(self):
        pass

    @abstractmethod
    def run(self):
        pass

    @classmethod
    @abstractmethod
    def from_yaml(self):
        pass


@dataclass
class ExpensesTracker(Tracker):
    telegram: TelegramConnection
    gsheet: GSheetConnection
    chat_id: int
    spreadsheet_id: int
    name: str = EXPENSES_TRACKER_NAME
    month_names = {
        1: "jan", 2: "fev", 3: "mar", 4: "abr", 5: "mai", 6: "jun", 7: "jul",
        8: "ago", 9: "set", 10: "out", 11: "nov", 12: "dez"
    }
    help_message = EXPENSES_TRACKER_HELP_MESSAGE

    def __post_init__(self):
        self.to_upload: list[ExpenseMessage] = []
        self.failed_to_parse: list[Message] = []
        self.asked_for_help: list[Message] = []

    @classmethod
    def from_yaml(cls, file_path: str = "settings.yaml"):
        tel = TelegramConnection.from_yaml(file_path)
        gsheet = GSheetConnection()
        with open(file_path, "r", encoding="UTF-8") as f:
            cfg = yaml.safe_load(f)
            chat_id = load_chat_id(EXPENSES_TRACKER_NAME, file_path)
            sheet_id = load_sheet_id(EXPENSES_TRACKER_NAME, file_path)
        return cls(tel, gsheet, chat_id, sheet_id)

    def fetch_telegram_messages(self) -> list[Message]:
        """
        logger.info("Probing group %s (%d) for new messages",
                    chat.name, chat.id)
        help_wanted: list[int] = []
        help_given: set[int] = set()
        async for msg in self.client.iter_messages(chat.id, limit=100):
            if msg.text == "?":
                help_wanted.append(msg.id)
            elif msg.is_reply:
                already_replied.add(msg.reply_to.reply_to_msg_id)
            elif msg.text is None or msg.text.startswith("#"):
                continue
            else:
                chat.process_message(msg)

        need_help = [i for i in help_wanted if i not in help_given]
        if need_help:
            await self._send(chat.id, chat.help, need_help[0])
        """
        return asyncio.get_event_loop().run_until_complete(self.telegram.fetch_msgs(self.chat_id))

    def send_telegram_message(self, chat_id: int, txt: str,
                              reply_to: Optional[int]):
        asyncio.get_event_loop().run_until_complete(self.telegram.send_msg(chat_id, txt, reply_to))

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

        self.asked_for_help.extend(
            [msg for msg in help_wanted if msg.id not in answered]
        )
        return

    def treat_help_messages(self, msgs: list[Message]) -> list[Message]:
        pass

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
                [dt_fmt(m.date), m.counterparty, m.description, a, dec_fmt(p), m.id]
                for a, p in [("Despesa", m.price), (m.account, -m.price)]
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

        text = (
            "# MENSAGEM AUTOMATICA #\n\n" +
            (f"{success_text}\n\n{ok_msgs}\n\n\n" if ok_msgs else "") +
            (f"{failure_text}\n\n{nok_msgs}\n\n\n" if nok_msgs else "") +
            (f"{spent_text}" if ok_msgs else "")
        )
        self.send_telegram_message(self.chat_id, text, None)
        if self.asked_for_help:
            self.send_telegram_message(
                self.chat_id, self.help_msg, max(self.asked_for_help)
            )
        # TODO: add total expenses as of this month

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
    """
    tel = TelegramConnection.from_yaml()
    gsheet = GSheetConnection()
    exp_tracker_name = "expenses"
    exp_chat_id = load_chat_id(exp_tracker_name)
    exp_sheet_id = load_sheet_id(exp_tracker_name)
    exp_tracker = ExpensesTracker(tel, gsheet, exp_chat_id, exp_sheet_id)
    exp_tracker.fetch_gsheet_state()
    # TODO: clean state
    exp_tracker.process_received_messages()
    exp_tracker.update_gsheet_state()
    exp_tracker.send_feedback_messages()
    exp_tracker.clean_local_state()
    """
    load_and_run_trackers([ExpensesTracker])
