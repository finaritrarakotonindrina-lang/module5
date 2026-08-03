from abc import ABC, abstractmethod
import typing


class DataProcessor(ABC):
    def __init__(self) -> None:
        self.rank: int = 0
        self.data: list[tuple[int, str]] = []
        self.total_processed: int = 0

    @abstractmethod
    def validate(self, data: typing.Any) -> bool:
        pass

    @abstractmethod
    def ingest(self, data: typing.Any) -> None:
        pass

    def output(self) -> tuple[int, str]:
        if not self.data:
            raise ValueError("not valid !")
        return self.data.pop(0)


class NumericProcessor(DataProcessor):
    def validate(self, data: typing.Any) -> bool:
        if isinstance(data, bool):
            return False
        elif isinstance(data, (int, float)):
            return True
        elif isinstance(data, list):
            if not data:
                return False
            return all(
                not isinstance(value, bool) and isinstance(value, (int, float))
                for value in data
            )
        return False

    def ingest(self, data: int | float | typing.Sequence[int | float]) -> None:
        if isinstance(data, list):
            for value in data:
                if self.validate(value):
                    self.data.append((self.rank, str(value)))
                    self.rank += 1
                    self.total_processed += 1
                else:
                    print(f"Skipping invalid numeric value: {value}")
            return
        if not self.validate(data):
            raise ValueError("Improper numeric data")
        self.data.append((self.rank, str(data)))
        self.rank += 1
        self.total_processed += 1


class TextProcessor(DataProcessor):
    def validate(self, data: typing.Any) -> bool:
        if isinstance(data, str):
            return True
        elif isinstance(data, list):
            return all(isinstance(value, str) for value in data)
        return False

    def ingest(self, data: str | list[str]) -> None:
        if isinstance(data, list):
            for value in data:
                if self.validate(value):
                    self.data.append((self.rank, value))
                    self.rank += 1
                    self.total_processed += 1
                else:
                    print(f"Skipping invalid text value: {value}")
            return
        if not self.validate(data):
            raise ValueError("Improper text data")
        self.data.append((self.rank, data))
        self.rank += 1
        self.total_processed += 1


class LogProcessor(DataProcessor):
    def validate(self, data: typing.Any) -> bool:
        if isinstance(data, dict):
            return all(
                isinstance(k, str) and isinstance(v, str)
                for k, v in data.items()
            )
        elif isinstance(data, list):
            return all(
                isinstance(item, dict) and self.validate(item)
                for item in data
            )
        return False

    def ingest(self, data: dict[str, str] | list[dict[str, str]]) -> None:
        if isinstance(data, dict):
            if not self.validate(data):
                raise ValueError("Improper dictionary data")
            log_entry = ": ".join(data.values())
            self.data.append((self.rank, log_entry))
            self.rank += 1
            self.total_processed += 1
        elif isinstance(data, list):
            for item in data:
                if self.validate(item):
                    log_entry = ": ".join(item.values())
                    self.data.append((self.rank, log_entry))
                    self.rank += 1
                    self.total_processed += 1
                else:
                    print(f"Skipping invalid dictionary: {item}")


class DataStream:
    def __init__(self) -> None:
        self.processors: list[DataProcessor] = []

    def register_processor(self, proc: DataProcessor) -> None:
        self.processors.append(proc)

    def process_stream(self, stream: list[typing.Any]) -> None:
        for item in stream:
            processed = False
            for proc in self.processors:
                if proc.validate(item):
                    try:
                        proc.ingest(item)
                        processed = True
                        break
                    except ValueError:
                        print("datastream error\n")
            if not processed:
                print("DataStream error"
                      f" - Can't process element in stream: {item}")

    def print_processors_stats(self) -> None:
        if not self.processors:
            print("No processor found, no data")
            return
        for proc in self.processors:
            name = proc.__class__.__name__.replace("Processor", " Processor")
            total = proc.total_processed
            remaining = len(proc.data)
            print(f"{name}: total {total} items processed, "
                  f"remaining {remaining} on processor")


def main() -> None:
    print("=== Code Nexus - Data Stream ===")
    print("")
    print("Initialize Data Stream..")
    print("== DataStream statistics ==")
    stream = DataStream()
    stream.print_processors_stats()
    print("")
    print("Registering Numeric Processor")
    print("")
    num_proc = NumericProcessor()
    stream.register_processor(num_proc)
    batch: list[typing.Any] = [
        'Hello world',
        [3.14, 1, 2.71],
        [{'log_level': 'WARNING',
          'log_message': 'Telnet access! Use ssh instead'},
         {'log_level': 'INFO', 'log_message': 'User wil is connected'}],
        42,
        ['Hi', 'five']
    ]
    print(f"Send first batch of data on stream: {batch}")
    stream.process_stream(batch)
    print("== DataStream statistics ==")
    stream.print_processors_stats()
    print("")
    print("Registering other data processors")
    text_proc = TextProcessor()
    log_proc = LogProcessor()
    stream.register_processor(text_proc)
    stream.register_processor(log_proc)
    print("Send the same batch again")
    stream.process_stream(batch)
    print("== DataStream statistics ==")
    stream.print_processors_stats()
    print("")
    num_to_consume = min(3, len(num_proc.data))
    text_to_consume = min(2, len(text_proc.data))
    log_to_consume = min(1, len(log_proc.data))
    print(f"Consume some elements from the data processors: "
          f"Numeric {num_to_consume}, Text {text_to_consume},"
          f"Log {log_to_consume}")
    for _ in range(num_to_consume):
        num_proc.output()
    for _ in range(text_to_consume):
        text_proc.output()
    for _ in range(log_to_consume):
        log_proc.output()
    print("== DataStream statistics ==")
    stream.print_processors_stats()


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(e)
