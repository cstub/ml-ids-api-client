import click
import pandas as pd
import shutil
from tabulate import tabulate
from ml_ids_api_client.data import Selection, RandomSelection
from typing import Dict, Union, Optional, Callable

QUIT_CHAR = 'q'
RANDOM_CHAR = 'r'

cols, _ = shutil.get_terminal_size()
pd.set_option('display.max_columns', 100)
pd.set_option('display.max_rows', 1000)
pd.set_option('display.width', cols)


def prompt_for_selection(categories: Dict[int, str]) -> Optional[Union[Selection, RandomSelection]]:
    cat = prompt_for_category_selection(categories)

    if cat == QUIT_CHAR:
        return None

    nr_samples = prompt_for_sample_count_selection()
    delay = prompt_for_delay_selection()

    if cat == RANDOM_CHAR:
        return RandomSelection(nr_samples, delay)

    return Selection(cat, nr_samples, delay)


def prompt_for_category_selection(categories: Dict[int, str]) -> Union[str, int]:
    def validate_input(char: str) -> bool:
        return (char == QUIT_CHAR) or (char == RANDOM_CHAR) or (char.isdigit() and 0 <= int(char) <= max_cat)

    print_category_selection(categories)

    max_cat = len(categories) - 1
    prompt_msg = 'Select a category [0 - {}], press[{}] to use a random selection or press [{}] to quit the ' \
                 'program: '.format(max_cat, RANDOM_CHAR, QUIT_CHAR)
    invalid_msg = 'Invalid input. Must either be [{}], [{}] or a value between [0 - {}].' \
        .format(QUIT_CHAR, RANDOM_CHAR, max_cat)

    c = prompt(prompt_msg=prompt_msg,
               invalid_msg=invalid_msg,
               text='Category',
               is_valid_fn=validate_input)

    if c == QUIT_CHAR:
        return QUIT_CHAR

    if c == RANDOM_CHAR:
        return RANDOM_CHAR

    category = int(c)
    return categories[category]


def print_category_selection(categories: Dict[int, str]) -> None:
    click.echo('Network Traffic Categories:')
    for idx, cat in categories.items():
        print('{} - {}'.format(idx, cat))


def prompt_for_sample_count_selection() -> int:
    def validate_input(char: str) -> bool:
        return char.isdigit()

    c = prompt(prompt_msg='Select the number of samples: ',
               invalid_msg='Invalid input. Must be a positive integer.',
               text='Number samples',
               is_valid_fn=validate_input)

    return int(c)


def prompt_for_delay_selection() -> Optional[int]:
    c = prompt(prompt_msg='Select an optional delay in ms or press any key: ',
               invalid_msg='Invalid input. Must be a positive integer.',
               text='Delay',
               is_valid_fn=lambda x: True,
               default='')

    if c.isdigit():
        return int(c)

    return None


def prompt(prompt_msg: str, invalid_msg: str, text: str, is_valid_fn: Callable[[str], bool], default=None) -> str:
    while True:
        click.echo(prompt_msg)
        user_input = click.prompt(text=text, default=default).rstrip()

        if is_valid_fn(user_input):
            return user_input

        click.echo(invalid_msg)


def show_prediction_results(result_df: pd.DataFrame, accuracy: float, display_overflow: str) -> None:
    click.echo()
    click.echo('Prediction Results:')
    click.echo('===================')
    click.echo('Accuracy: {:.2f}%'.format(accuracy))
    click.echo('\nDetails:')
    click.echo('--------')
    print_dataframe(result_df, display_overflow)
    click.echo()


def print_dataframe(df: pd.DataFrame, display_overflow: str) -> None:
    if display_overflow == 'WRAP':
        click.echo(df.reset_index(drop=True))
    else:
        click.echo(tabulate(df, headers='keys', showindex=False))
