from abc import ABC, abstractmethod
from dataclasses import dataclass
import datetime as dt
from decimal import Decimal
import logging
import time
from typing import Optional

from telethon import TelegramClient
from telethon.tl.patched import Message
import yaml


logger = logging.getLogger(__name__)


@dataclass
class Expense:
    """TODO: add docstring"""
    price: Decimal
    counterparty: str
    description: str
    account: str
    date: dt.date
    chat_id: int
    msg_id: int

    def __str__(self):
        return (f"{self.price} - {self.counterparty} - "
                f"{self.description} - {self.account} - "
                f"{self.date.strftime('%Y-%m-%d')}")

    @classmethod
    def parse(cls, msg: Message):
        items = [i.strip() for i in msg.text.split("-")]
        price = Decimal(items[0].replace(",", "."))
        cp = items[1]
        desc = items[2]
        acc = items[3]
        date = (msg.date.date() if len(items) < 5 else
                dt.datetime.strptime(items[4], "%d/%m/%Y"))
        return cls(price, cp, desc, acc, date, msg.chat_id, msg.id)


class Chat(ABC):
    id: int
    name: str
    help: str

    @abstractmethod
    def process_message(self, msg: Message) -> None:
        pass


class ExpenseChat(Chat):
    def __init__(self, chat_id: int, name: str):
        self.id = chat_id
        self.name = name
        self.help: str = "TODO: write help msg"
        self.expenses: list[Expense] = []

    def process_message(self, msg: Message):
        self.expenses.append(Expense.parse(msg))

    def fetch_gsheet_state(self):
        pass

    def to_gsheet(self):
        pass


class TelegramWatcher:
    """TODO: add docstring"""
    def __init__(self, name: str, api_id: int, api_hash: str,
                 chats: list[Chat]):
        self.name = name
        self.id = api_id
        self.hash = api_hash
        self.chats = chats
        self.client = TelegramClient(name, api_id, api_hash)

    @classmethod
    def from_yaml(cls, yaml_path: str = "settings.yaml"):
        """TODO: add docstring"""
        logger.info("Loading Telegram config from '%s'", yaml_path)
        chat_type = {"expenses": ExpenseChat}
        with open(yaml_path, "r", encoding="UTF-8") as f:
            cfg = yaml.safe_load(f)
            api_id = cfg["telegram"]["client"]["id"]
            api_hash = cfg["telegram"]["client"]["hash"]
            name = cfg["telegram"]["client"]["name"]
            chats = [
                chat_type[chattype](chat_id, chat_name)
                for chattype, chats in cfg["telegram"]["chats"].items()
                for chat_name, chat_id in chats.items()
            ]
            logger.debug("name: %s; id: %s, hash: %s",
                         name, api_id, api_hash)
            logger.info("Connecting to the following chats: %s",
                        ", ".join([c.name for c in chats]))
            return cls(name, api_id, api_hash, chats)

    async def _send(self, chat_id: int, txt: str, reply_to: Optional[int]):
        logger.info("Sending to chat %d: %s", chat_id, txt)
        await self.client.send_message(
            entity=chat_id,
            message=txt,
            reply_to=reply_to
        )

    async def fetch_messages(self, chat: Chat):
        """TODO: add docstring"""
        logger.info("Probing group %s (%d) for new messages",
                    chat.name, chat.id)
        asked_for_help: list[int] = []
        already_replied: set[int] = set()
        async for msg in self.client.iter_messages(chat.id, limit=100):
            if msg.text == "?":
                asked_for_help.append(msg.id)
            elif msg.is_reply:
                already_replied.add(msg.reply_to.reply_to_msg_id)
            elif msg.text is None or msg.text.startswith("#"):
                continue
            else:
                chat.process_message(msg)

        non_answered = [i for i in asked_for_help
                        if i not in already_replied]
        if non_answered:
            await self._send(chat.id, chat.help, non_answered[0])

    def watch(self, time_interval: int = 15):
        """TODO: add docstring"""
        logger.info("Watching telegram for untreated messages")
        logger.debug("Sleeping for %d seconds between checks", time_interval)
        client = self.client
        while True:
            for chat in self.chats:
                with client:
                    client.loop.run_until_complete(self.fetch_messages(chat))
                    # state_info = chat.update_state()
                    # await self._send(chat.id, state_info, None)
            time.sleep(time_interval)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    w = TelegramWatcher.from_yaml()
    w.watch()
