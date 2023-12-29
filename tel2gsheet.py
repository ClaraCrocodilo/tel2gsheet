from abc import ABC, abstractmethod
from dataclasses import dataclass, field
import datetime as dt
import logging
import os.path
from typing import Optional, Type

from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
import pandas as pd
from telethon import TelegramClient
from telethon.tl.patched import Message
import yaml


logger = logging.getLogger(__name__)
GOOGLE_SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]


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
