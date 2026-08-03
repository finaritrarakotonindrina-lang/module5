from abc import ABC, abstractmethod
import typing


class DataProcessor(ABC):
    def __init__(self) -> None:
        self.rank: int = 0
        self.data: list[tuple[int, str]] = []

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
                else:
                    print(f"Skipping invalid numeric value: {value}")
            return
        if not self.validate(data):
            raise ValueError("Improper numeric data")
        self.data.append((self.rank, str(data)))
        self.rank += 1


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
        elif isinstance(data, list):
            for value in data:
                self.data.append((self.rank, value))
                self.rank += 1


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
        elif isinstance(data, list):
            for item in data:
                if self.validate(item):
                    log_entry = ": ".join(item.values())
                    self.data.append((self.rank, log_entry))
                    self.rank += 1
                else:
                    print(f"Skipping invalid dictionary: {item}")


def main() -> None:
    print("=== Code Nexus - Data Processor ===")
    print()
    print("Testing Numeric Processor...")
    test_num = NumericProcessor()
    to_fail = 42
    print(f"Trying to validate input '{to_fail}':"
          f" {test_num.validate(to_fail)}")
    print("Trying to validate input 'Hello': "
          f"{test_num.validate('Hello')}")
    print("Test invalid ingestion of string 'foo' without prior validation:")
    try:
        test_num.ingest('foo')
    except Exception as e:
        print(f"got exception: {e}")
    data = [1, 2, 3, 4, 5]
    print(f"Processing data: {data}")
    print("Extracting 3 values...")
    test_num.ingest(data)
    for _ in range(3):
        try:
            rank, val = test_num.output()
            print(f"Numeric value {rank}: {val}")
        except Exception:
            print("failed.")
            break

    print("\nTesting Text Processor..")
    test_text = TextProcessor()
    print(f"Trying to validate input '42': {test_text.validate(42)}")
    list_text = ['Hello', 'Nexus', 'World']
    print("Extracting 1 value...")
    test_text.ingest(list_text)
    try:
        for _ in range(1):
            rank, val = test_text.output()
            print(f"Text value {rank}: {val}")
    except Exception:
        print("No more values to extract.")

    print("\nTesting Log Processor...")
    test_log = LogProcessor()
    print(f"Trying to validate input 'Hello': {test_log.validate('hello')}")
    list_log = [
        {'log_level': 'NOTICE', 'log_message': 'Connection to server'},
        {'log_level': 'ERROR', 'log_message': 'Unauthorized access!!'}
        ]
    print(f"Processing data: {list_log}")
    print("Extracting 2 values...")
    test_log.ingest(list_log)
    try:
        for _ in range(2):
            rank, value = test_log.output()
            print(f"Log entry {rank}: {value}")
    except Exception:
        print("invalid!")


if __name__ == "__main__":
    main()
