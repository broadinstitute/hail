import json
import os
from typing import Optional, Union

import hail as hl
import pkg_resources


def load_dataset(name: str,
                 version: Optional[str],
                 reference_genome: Optional[str],
                 region: str = 'us',
                 cloud: str = 'gcp') -> Union[hl.Table, hl.MatrixTable]:
    """Load a genetic dataset from Hail's repository.

    Example
    -------
    >>> # Load the 1000_Genomes_autosomes MatrixTable with GRCh38 coordinates.
    >>> mt_1kg = hl.experimental.load_dataset(name='1000_Genomes_autosomes',   # doctest: +SKIP
    ...                                       version='phase_3',
    ...                                       reference_genome='GRCh38',
    ...                                       region='us',
    ...                                       cloud='gcp')

    Parameters
    ----------
    name : :class:`str`
        Name of the dataset to load.
    version : :class:`str`, optional
        Version of the named dataset to load (see available versions in
        documentation). Possibly ``None`` for some datasets.
    reference_genome : :class:`str`, optional
        Reference genome build, ``'GRCh37'`` or ``'GRCh38'``. Possibly ``None``
        for some datasets.
    region : :class:`str`
        Specify region for bucket, ``'us'`` or ``'eu'``, (default is ``'us'``).
    cloud : :class:`str`
        Specify if using Google Cloud Platform or Amazon Web Services,
        ``'gcp'`` or ``'aws'`` (default is ``'gcp'``).

    Note
    ----
    The ``'aws'`` `cloud` platform is currently only available for the ``'us'``
    `region`. If `region` is ``'eu'``, `cloud` must be set to ``'gcp'``.

    Returns
    -------
    :class:`.Table` or :class:`.MatrixTable`
    """

    valid_regions = {'us', 'eu'}
    if region not in valid_regions:
        raise ValueError(f'Specify valid region parameter,'
                         f' received: region={repr(region)}.\n'
                         f'Valid region values are {valid_regions}.')

    valid_clouds = {'gcp', 'aws'}
    if cloud not in valid_clouds:
        raise ValueError(f'Specify valid cloud parameter,'
                         f' received: cloud={repr(cloud)}.\n'
                         f'Valid cloud platforms are {valid_clouds}.')

    config_path = pkg_resources.resource_filename(__name__, 'datasets.json')
    assert os.path.exists(config_path), f'{config_path} does not exist'
    with open(config_path) as f:
        datasets = json.load(f)

    names = set([dataset for dataset in datasets])
    if name not in names:
        raise ValueError(f'{name} is not a dataset available in the'
                         f' repository.')

    versions = set(dataset['version'] for dataset in datasets[name]['versions'])
    if version not in versions:
        raise ValueError(f'Version {repr(version)} not available for dataset'
                         f' {repr(name)}.\n'
                         f'Available versions: {versions}.')

    reference_genomes = set(dataset['reference_genome']
                            for dataset in datasets[name]['versions'])
    if reference_genome not in reference_genomes:
        raise ValueError(f'Reference genome build {repr(reference_genome)} not'
                         f' available for dataset {repr(name)}.\n'
                         f'Available reference genome builds:'
                         f' {reference_genomes}.')

    clouds = set(k for dataset in datasets[name]['versions']
                 for k in dataset['url'].keys())
    if cloud not in clouds:
        raise ValueError(f'Cloud platform {repr(cloud)} not available for'
                         f' dataset {name}.\n'
                         f'Available platforms: {clouds}.')

    regions = set(k for dataset in datasets[name]['versions']
                  for k in dataset['url'][cloud].keys())
    if region not in regions:
        raise ValueError(f'Region {repr(region)} not available for dataset'
                         f' {repr(name)} on cloud platform {repr(cloud)}.\n'
                         f'Available regions: {regions}.')

    path = [dataset['url'][cloud][region]
            for dataset in datasets[name]['versions']
            if all([dataset['version'] == version,
                    dataset['reference_genome'] == reference_genome])]
    assert len(path) == 1
    path = path[0]

    if path.endswith('.ht'):
        dataset = hl.read_table(path)
    else:
        if not path.endswith('.mt'):
            raise ValueError(f'Invalid path {repr(path)}: can only load'
                             f' datasets with .ht or .mt extensions.')
        dataset = hl.read_matrix_table(path)

    return dataset
