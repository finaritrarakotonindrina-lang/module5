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
        if not self.validate(data):
            raise ValueError("Improper texte data")
        if isinstance(data, str):
            self.data.append((self.rank, data))
            self.rank += 1
            self.total_processed += 1
        elif isinstance(data, list):
            for value in data:
                self.data.append((self.rank, value))
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


class ExportPlugin(typing.Protocol):
    def process_output(self, data: list[tuple[int, str]]) -> None:
        pass


class CSVExportPlugin:
    def process_output(self, data: list[tuple[int, str]]) -> None:
        values = ",".join(value for rank, value in data)
        print(f"CSV Output:\n{values}")


class JSONExportPlugin:
    def process_output(self, data: list[tuple[int, str]]) -> None:
        pairs = ", ".join(
            f'"item_{rank}": "{value}"' for rank, value in data
        )
        print(f"JSON Output:\n{{{pairs}}}")


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

    def output_pipeline(self, nb: int, plugin: ExportPlugin) -> None:
        for proc in self.processors:
            nb_available = min(nb, len(proc.data))
            if nb_available <= 0:
                continue
            batch: list[tuple[int, str]] = [
                proc.output() for _ in range(nb_available)
            ]
            plugin.process_output(batch)

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
    print("=== Code Nexus - Data Pipeline ===")
    print("")
    print("Initialize Data Stream...")
    print("")
    print("== DataStream statistics ==")
    stream = DataStream()
    stream.print_processors_stats()
    print("")
    print("Registering Processors")
    print("")
    num_proc = NumericProcessor()
    text_proc = TextProcessor()
    log_proc = LogProcessor()
    stream.register_processor(num_proc)
    stream.register_processor(text_proc)
    stream.register_processor(log_proc)

    batch1: list[typing.Any] = [
        'Hello world',
        [3.14, -1, 2.71],
        [{'log_level': 'WARNING',
          'log_message': 'Telnet access! Use ssh instead'},
         {'log_level': 'INFO', 'log_message': 'User wil is connected'}],
        42,
        ['Hi', 'five']
    ]
    print(f"Send first batch of data on stream: {batch1}")
    stream.process_stream(batch1)
    print("")
    print("== DataStream statistics ==")
    stream.print_processors_stats()
    print("")

    print("Send 3 processed data from each processor to a CSV plugin:")
    csv_plugin = CSVExportPlugin()
    stream.output_pipeline(3, csv_plugin)
    print("")
    print("== DataStream statistics ==")
    stream.print_processors_stats()
    print("")

    batch2: list[typing.Any] = [
        21,
        ['I love AI', 'LLMs are wonderful', 'Stay healthy'],
        [{'log_level': 'ERROR', 'log_message': '500 server crash'},
         {'log_level': 'NOTICE',
          'log_message': 'Certificate expires in 10 days'}],
        [32, 42, 64, 84, 128, 168],
        'World hello'
    ]
    print(f"Send another batch of data: {batch2}")
    stream.process_stream(batch2)
    print("")
    print("== DataStream statistics ==")
    stream.print_processors_stats()
    print("")

    print("Send 5 processed data from each processor to a JSON plugin:")
    json_plugin = JSONExportPlugin()
    stream.output_pipeline(5, json_plugin)
    print("")
    print("== DataStream statistics ==")
    stream.print_processors_stats()


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(e)
