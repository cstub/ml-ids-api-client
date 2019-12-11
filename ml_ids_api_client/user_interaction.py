"""
Utility functions for CLI applications.
"""
from typing import Dict, Union, Optional, Callable
import shutil
import click
import pandas as pd
from tabulate import tabulate
from ml_ids_api_client.data import Selection, RandomSelection

QUIT_CHAR = 'q'
RANDOM_CHAR = 'r'

COLS, _ = shutil.get_terminal_size()
pd.set_option('display.max_columns', 100)
pd.set_option('display.max_rows', 1000)
pd.set_option('display.width', COLS)


def prompt_for_selection(categories: Dict[int, str]) -> Optional[Union[Selection, RandomSelection]]:
    """
    Prompts the user to input a network traffic category selection.

    :param categories: Network traffic categories.
    :return: `Selection` or `RandomSelection` if the user chose a valid selection.
             None if the user selected the option to quit the application.
    """
    cat = prompt_for_category_selection(categories)

    if cat == QUIT_CHAR:
        return None

    nr_samples = prompt_for_sample_count_selection()
    delay = prompt_for_delay_selection()

    if cat == RANDOM_CHAR:
        return RandomSelection(nr_samples, delay)

    return Selection(cat, nr_samples, delay)


def prompt_for_category_selection(categories: Dict[int, str]) -> Union[str, int]:
    """
    Prompts the user to input a network traffic category selection.

    :param categories: Network traffic categories.
    :return: QUIT_CHAR if the user selected the option to quit the application.
             RANDOM_CHAR if the user selected the random selection.
             Category id if the user selected a valid category.
    """

    def validate_input(char: str) -> bool:
        return (char == QUIT_CHAR) or (char == RANDOM_CHAR) or (char.isdigit() and 0 <= int(char) <= max_cat)

    print_category_selection(categories)

    max_cat = len(categories) - 1
    prompt_msg = 'Select a category [0 - {}], press[{}] to use a random selection or press [{}] to quit the ' \
                 'program: '.format(max_cat, RANDOM_CHAR, QUIT_CHAR)
    invalid_msg = 'Invalid input. Must either be [{}], [{}] or a value between [0 - {}].' \
        .format(QUIT_CHAR, RANDOM_CHAR, max_cat)

    user_input = prompt(prompt_msg=prompt_msg,
                        invalid_msg=invalid_msg,
                        text='Category',
                        is_valid_fn=validate_input)

    if user_input == QUIT_CHAR:
        return QUIT_CHAR

    if user_input == RANDOM_CHAR:
        return RANDOM_CHAR

    category = int(user_input)
    return categories[category]


def print_category_selection(categories: Dict[int, str]) -> None:
    """
    Prints the available network traffic categories.
    :param categories: Network traffic categories.
    :return: None
    """
    click.echo('Network Traffic Categories:')
    for idx, cat in categories.items():
        print('{} - {}'.format(idx, cat))


def prompt_for_sample_count_selection() -> int:
    """
    Prompts the user to input the number of samples that should be selected.

    :return: Number of samples.
    """

    def validate_input(char: str) -> bool:
        return char.isdigit()

    user_input = prompt(prompt_msg='Select the number of samples: ',
                        invalid_msg='Invalid input. Must be a positive integer.',
                        text='Number samples',
                        is_valid_fn=validate_input)

    return int(user_input)


def prompt_for_delay_selection() -> Optional[int]:
    """
    Prompts the user to input the delay that should be selected.

    :return: Delay if selected, else None.
    """
    user_input = prompt(prompt_msg='Select an optional delay in ms or press any key: ',
                        invalid_msg='Invalid input. Must be a positive integer.',
                        text='Delay',
                        is_valid_fn=lambda x: True,
                        default='')

    if user_input.isdigit():
        return int(user_input)

    return None


def prompt(prompt_msg: str, invalid_msg: str, text: str, is_valid_fn: Callable[[str], bool], default=None) -> str:
    """
    Generic method that prompts the user for an input.

    :param prompt_msg: Message to display to the user.
    :param invalid_msg: Message to display upon invalid input.
    :param text: Text to display on the input field.
    :param is_valid_fn: Function to determine if the user input is valid.
    :param default: Default value for the input.
    :return: User input as string.
    """
    while True:
        click.echo(prompt_msg)
        user_input = click.prompt(text=text, default=default).rstrip()

        if is_valid_fn(user_input):
            return user_input

        click.echo(invalid_msg)


def show_prediction_results(result_df: pd.DataFrame, accuracy: float, display_overflow: str) -> None:
    """
    Displays prediction results to the user.

    :param result_df: Pandas DataFrame containing the prediction results.
    :param accuracy: Accuracy of the prediction results.
    :param display_overflow: Defines the overflow behaviour if the output exceeds the window width (WRAP | NOWRAP).
    :return: None
    """
    click.echo()
    click.echo('Prediction Results:')
    click.echo('===================')
    click.echo('Accuracy: {:.2f}%'.format(accuracy))
    click.echo('\nDetails:')
    click.echo('--------')
    print_dataframe(result_df, display_overflow)
    click.echo()


def print_dataframe(df: pd.DataFrame, display_overflow: str) -> None:
    """
    Prints a Pandas DataFrame to the standard output.

    :param df: Pandas DataFrame.
    :param display_overflow: Defines the overflow behaviour if the output exceeds the window width (WRAP | NOWRAP).
    :return:
    """
    if display_overflow == 'WRAP':
        click.echo(df.reset_index(drop=True))
    else:
        click.echo(tabulate(df, headers='keys', showindex=False))
