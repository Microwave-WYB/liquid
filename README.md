# ktflow

A simple, functional flow library for Python that allows method chaining for data processing operations, inspired by Kotlin's sequence operations.

## Features

- Method chaining for common operations like `map`, `filter`, `reduce`
- Support for parallel processing with executors
- Error handling with recovery functions
- Simple, intuitive API inspired by Kotlin's sequence operations and flow API
- Type hints for better IDE experience

## Installation

```bash
pip install git+https://github.com/Microwave-WYB/ktflow.git
```

## Hello World Example

```python
from ktflow import Flow

print(
    Flow.of("Hello", "World")
    .on_each(print)
    .map(lambda x: x.upper())
    .reduce(lambda acc, x: acc + " " + x)
)
```

Output:
```
Hello
World
HELLO WORLD
```

## Advanced Example

```python
from concurrent.futures import ThreadPoolExecutor
from ktflow import Flow

with ThreadPoolExecutor(max_workers=4) as executor:
    result = (
        Flow.of(1, 2, 3, 4, 5)
        .filter(lambda x: x % 2 == 0)  # Keep only even numbers
        .submit_map(executor, lambda x: x * 10)  # Process in parallel
        .map(lambda x: f"Number: {x}")  # Transform results
        .to_list()
    )

print(result)  # ['Number: 20', 'Number: 40']
```

## Error Handling

```python
def process(x):
    if x < 0:
        raise ValueError("Negative value")
    return x * 10

def handle_error(item, error):
    print(f"Error processing {item}: {error}")
    yield 0  # similar to Kotlin's emit(0)

result = Flow.of(-1, 2, 3).map(process, handle_error).to_list()
print(result)  # [0, 20, 30]
```

## Demo

You can run the demo script using `uv`:

```bash
git clone https://github.com/Microwave-WYB/ktflow.git
cd ktflow
uv run demo.py
```
