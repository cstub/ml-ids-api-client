import click
from time import sleep

from ml_ids_api_client.data import load_dataset, get_categories, select_samples, merge_predictions
from ml_ids_api_client.user_interaction import prompt_for_selection, show_prediction_results
from ml_ids_api_client.http_client import call_predict_api


@click.command()
@click.option('--dataset-uri', type=str, required=True,
              help='Dataset uri. Can either be a local path [file://] or an S3 url [s3://].')
@click.option('--api-url', type=str, required=True,
              help='URL of the prediction API server.')
@click.option('--s3-local-storage-path', type=click.Path(), default='../output/s3_dataset.h5',
              help='Local path used to store the downloaded dataset from S3.')
def run_client(dataset_uri, api_url, s3_local_storage_path):
    click.echo('Loading dataset...')
    dataset = load_dataset(dataset_uri, s3_local_storage_path)
    categories = get_categories(dataset)

    while True:
        selection = prompt_for_selection(categories)

        if selection is None:
            break

        samples = select_samples(dataset, selection)

        if selection.delay is None:
            predictions = call_predict_api(api_url, samples)
        else:
            predictions = []
            for i in range(0, selection.nr_samples):
                sample = samples.iloc[i:i + 1].copy()
                click.echo('Requesting prediction [{}] for label [{}]...'
                           .format(i + 1, sample.at[sample.index.to_list()[0], 'label']))
                pred = call_predict_api(api_url, sample)
                predictions.extend(pred)
                sleep(selection.delay / 1000)

        results, acc = merge_predictions(samples, predictions)
        show_prediction_results(results, acc)


if __name__ == '__main__':
    run_client()
