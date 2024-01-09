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
    load_chat_id, load_sheet_id, logger
)


TEXT_HEADER = "# MENSAGEM AUTOMATICA #\n\n"
HELP_MESSAGE = (
    "As mensagens de consumo de comida devem ter o seguinte formato:\n"
    "NOME_DA_COMIDA - NUMERO_UNIDADES UNIDADE\n"
    "NOME_DA_COMIDA - Nome que identifica a comida, e.g., 'Mini Pão Swift'\n"
    "NUMERO_UNIDADES - Quantidade de unidades consumidas, e.g., 3, 0.2, 1/2\n"
    "UNIDADE - Unidade de medida usada, e.g., g, ml, un\n"
    "Ex:\n"
    "Batata Frita - 60g\n"
    "Barra de Cereal Dia - 1/2 un\n"
    "Suco de Laranja - 1 copo\n\n"
    "As mensagens de registro de caloria por unidade devem ter o seguinte "
    "formato:\n"
    "@ NOME_DA_COMIDA - NUMERO_CALORIAS cal/NUMERO_UNIDADES UNIDADE\n"
    "NOME_DA_COMIDA - Nome que identifica a comida, e.g., 'Mini Pão Swift'\n"
    "NUMERO_CALORIAS - Valor numerico com ponto como separador decimal, e.g., "
    "350, 72.3\n"
    "NUMERO_UNIDADES - Quantidade de unidades referentes à quantidade de "
    "calorias, e.g., 3, 0.2, 1/2\n"
    "UNIDADE - Unidade de medida utilizada, e.g., g, ml, un\n"
    "Ex:\n"
    "@ Pizza Lombo Sadia - 1200 cal/1 un\n"
    "@ Mini Pão Swift - 124 cal/50g\n\n"
    "É possível anotar diversos registros de consumo ou de caloria por "
    "unidade numa única mensagem do Telegram. Basta inserir cada registro em "
    "uma linha (i.e., clicando em 'Enter' no teclado)"
)
TRACKER_NAME = "calories"


def normalize_text(text: str) -> str:
    return unidecode(text.lower().strip())


def split_unit(text: str) -> tuple[Decimal, str]:
    for i, e in list(enumerate(text))[::-1]:
        if e.isnumeric():
            val = text[:i+1].strip()
            un = text[i+1:].strip()
            if "/" in val:
                p, q = val.split("/")
                return Decimal(p)/Decimal(q), un
            return Decimal(val), un
    raise ValueError("Couldn't find a pair of value and unit of measurement")


@dataclass
class CaloriesStats:
    today_calories: Decimal
    monthly_calories: Decimal
    monthly_mean: float
    monthly_stddev: float


class MissingRegistrationError(ValueError):
    pass


@dataclass
class MissingRegistrationMessage(IncomingMessage):
    id: int
    date: dt.date
    chat_id: int
    text: str

    def __str__(self):
        return self.text

    @classmethod
    def parse(self):
        pass


@dataclass
class MissingRegistration():
    description: str
    unit: str

    def __str__(self):
        return f"{self.description} [{self.unit}]"


@dataclass
class CalorieRegistration(IncomingMessage):
    id: int
    date: dt.date
    chat_id: int
    description: str
    quantity: Decimal
    unit: str
    calories: Decimal

    def __str__(self):
        return (f"{self.description}: {self.calories} cal per {self.quantity} "
                f"{self.unit}")

    @classmethod
    def parse(cls, msg: Message, registrations = list[list[str, str]]):
        items = [i.strip() for i in msg.text.lstrip("@").split("-")]
        desc = items[0]
        date = msg.date.date()
        cals = Decimal(items[1].split("/")[0].replace("cal", "").strip())
        value, unit = split_unit(items[1].split("/")[1].strip())
        if (desc, unit) in registrations:
            raise ValueError(f"Registration for item '{desc}', unit '{unit}' "
                             "already exists")
        return cls(msg.id, date, msg.chat_id, desc, value, unit, cals)


@dataclass
class MealMessage(IncomingMessage):
    """TODO: add docstring"""
    id: int
    date: dt.date
    chat_id: int
    description: str
    quantity: Optional[Decimal]
    unit: Optional[str]
    calories: Optional[Decimal]

    def __post_init__(self):
        has_quantity = (self.quantity is not None and self.unit is not None)
        has_calories = (self.calories is not None)
        assert has_calories or has_quantity

    def __str__(self):
        qty = f" {self.quantity} [{self.unit}]" if self.quantity else ""
        cals = f" ({self.calories} cal)" if self.calories else ""
        date_str = self.date.strftime("%d/%m/%Y")
        return f"{self.description}:{qty}{cals} @ {date_str}"

    @classmethod
    def parse(cls, msg: Message, registrations = list[list[str, str]]):
        items = [i.strip() for i in msg.text.split("-")]
        desc = items[0]
        value, unit = split_unit(items[1])
        date = (msg.date.date() if len(items) < 5 else
                dt.datetime.strptime(items[2], "%d/%m/%Y"))
        if unit.upper() in ("CAL", "CALS"):
            return cls(msg.id, date, msg.chat_id, desc, None, None, value)
        else:
            normalized = [[d, un] for d, un in registrations
                          if normalize_text(d) == normalize_text(desc) and
                          normalize_text(un) == normalize_text(unit)]
            if not normalized:
                raise MissingRegistrationError("Missing registration for item "
                                               f" '{desc}', unit '{unit}'")
            n_desc, n_un = normalized[0][0], normalized[0][1]
            return cls(
                msg.id, date, msg.chat_id, n_desc, value, n_un, None
            )


@dataclass
class CaloriesTracker(Tracker):
    telegram: TelegramConnection
    gsheet: GSheetConnection
    chat_id: int
    spreadsheet_id: int
    name: str = TRACKER_NAME
    help_message = HELP_MESSAGE
    text_header = TEXT_HEADER

    def __post_init__(self):
        self.to_upload: list[MealMessage] = []
        self.new_registrations: list[CalorieRegistration] = []
        self.missing_registration: list[MissingRegistration] = []
        self.failed_to_parse: list[Message] = []
        self.asked_for_help: list[Message] = []

    @classmethod
    def from_yaml(cls, file_path: str = "settings.yaml"):
        logger.debug("Loading %s from '%s'", TRACKER_NAME, file_path)
        tel = TelegramConnection.from_yaml(file_path)
        gsheet = GSheetConnection()
        chat_id = load_chat_id(TRACKER_NAME, file_path)
        sheet_id = load_sheet_id(TRACKER_NAME, file_path)
        return cls(tel, gsheet, chat_id, sheet_id)

    def fetch_telegram_messages(self) -> list[Message]:
        logger.info("Fetching messages from chat %d", self.chat_id)
        return asyncio.get_event_loop().run_until_complete(
            self.telegram.fetch_msgs(self.chat_id)
        )

    def send_telegram_message(
            self, chat_id: int, txt: str, reply_to: Optional[int]
    ):
        logger.info("Sending to chat %d:\n%s", chat_id, txt)
        asyncio.get_event_loop().run_until_complete(
            self.telegram.send_msg(chat_id, txt, reply_to)
        )

    def process_received_messages(self):
        logger.info("Processing new Telegram messages")
        msgs = [m for m in self.fetch_telegram_messages()
                if m.id not in self.processed_msgs]
        if not msgs or msgs[0].text.startswith(TEXT_HEADER):
            logger.info("No new messages")
            return ([], [])
        msgs = sorted(
            msgs,
            key=lambda m: 0 if m.text and m.text.startswith("@") else 1
        )

        registrations = self.registrations_df[
            ["Comida", "Unidade"]
        ].values.tolist()

        help_wanted: list[int] = []
        answered: set[int] = set()
        logger.info("Parsing messages")
        for msg in msgs:
            original_text = msg.text
            if msg.is_reply:
                answered.add(msg.reply_to.reply_to_msg_id)
            if msg.text is None or msg.text.startswith("#"):
                continue
            elif msg.text.strip() == "?":
                help_wanted.append(msg)
            else:
                for line in original_text.splitlines():
                    msg.text = line
                    try:
                        if msg.text.startswith("@"):
                            reg = CalorieRegistration.parse(msg, registrations)
                            self.new_registrations.append(reg)
                            registrations.append(
                                [reg.description, reg.unit]
                            )
                        else:
                            self.to_upload.append(
                                MealMessage.parse(msg, registrations)
                            )
                    except MissingRegistrationError as e:
                        logger.info("Error parsing message: '%s'", msg.text)
                        logger.info(e)
                        self.missing_registration.append(
                            MissingRegistrationMessage(
                                msg.id, msg.date.date(), msg.chat_id, msg.text
                            )
                        )
                    except (IndexError, InvalidOperation, IndexError) as e:
                        logger.error("Error parsing message: '%s'", msg.text)
                        logger.debug(e)
                        self.failed_to_parse.append(msg)

        self.to_upload.reverse()
        self.new_registrations.reverse()
        self.missing_registration.reverse()
        self.failed_to_parse.reverse()
        self.asked_for_help.extend(
            [msg for msg in help_wanted if msg.id not in answered]
        )
        logger.info("Finished processing new messages")
        return

    def fetch_entries(self) -> pd.DataFrame:
        sheet_name = "Acompanhamento"
        sheet_range = "A:F"
        df = self.gsheet.read(self.spreadsheet_id, sheet_name, sheet_range)
        return df

    def fetch_registrations(self) -> pd.DataFrame:
        sheet_name = "Referência"
        sheet_range = "A:G"
        df = self.gsheet.read(self.spreadsheet_id, sheet_name, sheet_range)
        return df

    def fetch_processed_messages(self) -> set[int]:
        sheet_name = "Telegram"
        sheet_range = "A:B"
        df = self.gsheet.read(self.spreadsheet_id, sheet_name, sheet_range)
        return {int(i) for i in df["MsgId"].values if i}

    def fetch_gsheet_state(self):
        logger.info("Fetching GSheet state")
        self.entries_df = self.fetch_entries()
        self.registrations_df = self.fetch_registrations()
        self.processed_msgs = self.fetch_processed_messages()
        logger.info("Finished fetching GSheet state")

    def clean_local_state(self):
        self.to_upload = []
        self.new_registrations = []
        self.missing_registration = []
        self.failed_to_parse = []
        self.asked_for_help = []
        for attr in ("entries_df", "registrations_df", "processed_msgs"):
            if hasattr(self, attr):
                delattr(self, attr)

    def upload_processed_messages(self):
        sheet_name = "Telegram"
        sheet_range = "A:B"
        data = (
            [[msg.id, "SUCCESS"] for msg in self.to_upload] +
            [[msg.id, "SUCCESS"] for msg in self.new_registrations] +
            [[msg.id, "FAILURE"] for msg in self.failed_to_parse] +
            [[msg.id, "ASKED_FOR_HELP"] for msg in self.asked_for_help]
        )
        self.gsheet.write(self.spreadsheet_id, sheet_name, sheet_range, data)

    def upload_entries(self):
        cal_formula = ("=INDEX('Referência'!E:E, "
                       'MATCH(CONCAT(INDIRECT(CONCAT("B", ROW())), '
                       'INDIRECT(CONCAT("D", ROW()))), '
                       "'Referência'!F:F, 0))"
                       '*INDIRECT(CONCAT("C", ROW()))')
        sheet_name = "Acompanhamento"
        sheet_range = "A:F"
        data = []
        for m in self.to_upload:
            data.append([
                m.date.strftime("%d-%b-%Y"),
                m.description,
                str(m.quantity),
                m.unit,
                cal_formula if not m.calories else str(m.calories),
                m.id
            ])
        self.gsheet.write(self.spreadsheet_id, sheet_name, sheet_range, data)

    def upload_new_registrations(self):
        sheet_name = "Referência"
        sheet_range = "A:G"
        data = []
        cal_per_qty = ('=INDIRECT(CONCAT("B", ROW()))'
                       '/INDIRECT(CONCAT("C", ROW()))')
        key = ('=CONCAT(INDIRECT(CONCAT("A", ROW())), '
               'INDIRECT(CONCAT("D", ROW())))')
        for m in self.new_registrations:
            data.append([
                m.description,
                str(m.calories),
                str(m.quantity),
                m.unit,
                str(cal_per_qty),
                key,
                m.id
            ])
        self.gsheet.write(self.spreadsheet_id, sheet_name, sheet_range, data)

    def update_gsheet_state(self):
        logger.info("Uploading new data to GSheet")
        self.upload_entries()
        self.upload_new_registrations()
        self.upload_processed_messages()
        logger.info("Finished uploading to GSheet")

    def send_feedback_messages(self):
        if not (self.to_upload or self.failed_to_parse or self.asked_for_help):
            return

        df = self.fetch_entries()
        missing_reg_error = [
            MissingRegistration(v[0], v[1])
            for v in df[df["Calorias"] == "#N/A"][["Comida", "Unidade"]].values
        ]

        ok_source = self.to_upload + self.new_registrations
        ok_msgs = "\n\n".join([f"\t-> {str(m)}" for m in ok_source])
        nok_msgs = "\n\n".join(
            set(f"\t-> {m.text}" for m in self.failed_to_parse)
        )
        miss_msgs = "\n\n".join([f"\t-> {str(m)}" for m in
                                 self.missing_registration])
        error_msgs = "\n\n".join([f"\t-> {str(m)}" for m in missing_reg_error])


        success_text = "As seguintes mensagens foram registradas corretamente:"
        missing_text = ("As seguinte mensagens não foram registradas por falta "
                        "de registro de calorias:")
        failure_text = ("As seguintes mensagens não foram registradas por "
                        "erro de formatação:")
        error_text = ("As seguintes entradas apresentam erro por falta de "
                      "registro de calorias:")

        if ok_msgs:
            today = dt.datetime.now().date()
            date_str = dt.datetime.now().strftime("%m/%Y")
            stats = self.get_monthly_calories(today)
            cals_text = (f"No mês {date_str} (excluindo hoje) foram gastas "
                         f"{stats.monthly_calories:.2f} calorias "
                         f"(média {stats.monthly_mean:.2f} +- "
                         f"{stats.monthly_stddev:.2f}).\nHoje foram gastas "
                         f"{stats.today_calories:.2f} calorias.")
        else:
            cals_text = ""

        text = (
            self.text_header +
            (f"{success_text}\n\n{ok_msgs}\n\n\n" if ok_msgs else "") +
            (f"{missing_text}\n\n{miss_msgs}\n\n\n" if miss_msgs else "") +
            (f"{error_text}\n\n{error_msgs}\n\n\n" if error_msgs else "") +
            (f"{failure_text}\n\n{nok_msgs}\n\n\n" if nok_msgs else "") +
            cals_text +
            "\n\n" +
            "Em caso de dúvidas, envie uma mensagem contendo o caractere '?'"
        )
        if ok_msgs or nok_msgs or miss_msgs:
            self.send_telegram_message(self.chat_id, text, None)
        if self.asked_for_help:
            help_msg = self.text_header + self.help_message
            self.send_telegram_message(
                self.chat_id, help_msg, self.asked_for_help[-1].id
            )

    def get_monthly_calories(
        self, ref_date: dt.date
    ) -> CaloriesStats:
        self.fetch_gsheet_state()
        df = self.entries_df.copy()
        df["Calorias"] = pd.to_numeric(df["Calorias"])
        df["RefDate"] = df["Dia"].apply(
            lambda d: dt.datetime.strptime(d, "%d-%b-%Y").date()
        )
        today_cals = sum(df[df["RefDate"] == ref_date]["Calorias"].values)
        start_of_period = dt.date(ref_date.year, ref_date.month, 1)
        month_df = df[
            (df["RefDate"] < ref_date) & (df["RefDate"] >= start_of_period)
        ][["RefDate", "Calorias"]].groupby("RefDate").sum().reset_index()
        total = sum(month_df["Calorias"].values)
        mean = month_df["Calorias"].mean()
        std = month_df["Calorias"].std()
        return CaloriesStats(today_cals, total, mean, std)

    def run(self):
        logger.info("Running %s", self.__class__.__name__)
        self.fetch_gsheet_state()
        self.process_received_messages()
        self.update_gsheet_state()
        self.send_feedback_messages()
        self.clean_local_state()
        logger.info("Finished running %s", self.__class__.__name__)


def load_and_run_trackers(
    trackers: list[Type[Tracker]],
        yaml_file_path: str = "settings.yaml"
):
    logger.info("Loading and running the following trackers: %s",
                ", ".join([cls.__name__ for cls in trackers]))
    logger.debug("Using 'yaml_file_path' = %s", yaml_file_path)
    t_objs = [t.from_yaml(yaml_file_path) for t in trackers]
    logger.info("Loaded all trackers")
    for tracker in t_objs:
        logger.info("Running tracker %s", tracker.__class__.__name__)
        tracker.run()
    logger.info("Finished running all trackers. Exiting")


if __name__ == "__main__":
    today_str = dt.datetime.now().strftime("%Y-%m-%d_%Hh%M")
    log_name = f"{TRACKER_NAME}_{today_str}.log"
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        handlers=[
            logging.FileHandler(f"logs/{log_name}"),
            logging.StreamHandler()
        ]
    )
    logger.setLevel(level=logging.DEBUG)
    load_and_run_trackers([CaloriesTracker])
