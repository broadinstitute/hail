#!/opt/conda/default/bin/python3
import json
import os
import subprocess as sp
import sys
from subprocess import check_output

assert sys.version_info > (3, 0), sys.version_info

if sys.version_info >= (3, 7):
    def safe_call(*args):
        sp.run(args, capture_output=True, check=True)
else:
    def safe_call(*args):
        try:
            sp.check_output(args, stderr=sp.STDOUT)
        except sp.CalledProcessError as e:
            print(e.output).decode()
            raise e


def get_metadata(key):
    return check_output(['/usr/share/google/get_metadata_value', 'attributes/{}'.format(key)]).decode()


def mkdir_if_not_exists(path):
    try:
        os.makedirs(path)
    except OSError as e:
        if e.errno != os.errno.EEXIST:
            raise


# get role of machine (master or worker)
role = get_metadata('dataproc-role')

if role == 'Master':
    # additional packages to install
    pip_pkgs = [
        'setuptools',
        'mkl<2020',
        'ipywidgets<8',
        'jupyter_console<5',
        'nbconvert<6',
        'notebook<6',
        'qtconsole<5',
        'jupyter', 'tornado<6',  # https://github.com/hail-is/hail/issues/5505
        'lxml<5',
        'google-cloud==0.32.0',
        'ipython<7',
        'jgscm<0.2',
        'jupyter-spark',
    ]

    # add user-requested packages
    try:
        user_pkgs = get_metadata('PKGS')
    except:
        pass
    else:
        pip_pkgs.extend(user_pkgs.split('|'))

    print('pip packages are {}'.format(pip_pkgs))
    command = ['pip', 'install']
    command.extend(pip_pkgs)
    safe_call(*command)

    print('getting metadata')

    wheel_path = get_metadata('WHEEL')
    wheel_name = wheel_path.split('/')[-1]

    print('copying wheel')
    safe_call('gsutil', 'cp', wheel_path, f'/home/hail/{wheel_name}')

    safe_call('pip', 'install', '--no-dependencies', f'/home/hail/{wheel_name}')

    print('setting environment')

    spark_lib_base = '/usr/lib/spark/python/lib/'
    files_to_add = [os.path.join(spark_lib_base, x) for x in os.listdir(spark_lib_base) if x.endswith('.zip')]

    env_to_set = {
        'PYTHONHASHSEED': '0',
        'PYTHONPATH': ':'.join(files_to_add),
        'SPARK_HOME': '/usr/lib/spark/',
        'PYSPARK_PYTHON': '/opt/conda/default/bin/python',
        'PYSPARK_DRIVER_PYTHON': '/opt/conda/default/bin/python'
    }

    print('setting environment')

    for e, value in env_to_set.items():
        safe_call('/bin/sh', '-c',
                  'echo "export {}={}" | tee -a /etc/environment /usr/lib/spark/conf/spark-env.sh'.format(e, value))

    hail_jar = '/opt/conda/default/lib/python3.6/site-packages/hail/hail-all-spark.jar'
    conf_to_set = [
        'spark.executorEnv.PYTHONHASHSEED=0',
        # the below are necessary to make 'submit' work
        'spark.jars={}'.format(hail_jar),
        'spark.driver.extraClassPath={}'.format(hail_jar),
        'spark.executor.extraClassPath=./hail-all-spark.jar',
    ]

    print('setting spark-defaults.conf')

    with open('/etc/spark/conf/spark-defaults.conf', 'a') as out:
        out.write('\n')
        for c in conf_to_set:
            out.write(c)
            out.write('\n')

    # create Jupyter kernel spec file
    kernel = {
        'argv': [
            '/opt/conda/default/bin/python',
            '-m',
            'ipykernel',
            '-f',
            '{connection_file}'
        ],
        'display_name': 'Hail',
        'language': 'python',
        'env': env_to_set
    }

    # write kernel spec file to default Jupyter kernel directory
    mkdir_if_not_exists('/opt/conda/default/share/jupyter/kernels/hail/')
    with open('/opt/conda/default/share/jupyter/kernels/hail/kernel.json', 'w') as f:
        json.dump(kernel, f)

    # create Jupyter configuration file
    mkdir_if_not_exists('/opt/conda/default/etc/jupyter/')
    with open('/opt/conda/default/etc/jupyter/jupyter_notebook_config.py', 'w') as f:
        opts = [
            'c.Application.log_level = "DEBUG"',
            'c.NotebookApp.ip = "127.0.0.1"',
            'c.NotebookApp.open_browser = False',
            'c.NotebookApp.port = 8123',
            'c.NotebookApp.token = ""',
            'c.NotebookApp.contents_manager_class = "jgscm.GoogleStorageContentManager"'
        ]
        f.write('\n'.join(opts) + '\n')

    # setup jupyter-spark extension
    safe_call('/opt/conda/default/bin/jupyter', 'serverextension', 'enable', '--user', '--py', 'jupyter_spark')
    safe_call('/opt/conda/default/bin/jupyter', 'nbextension', 'install', '--user', '--py', 'jupyter_spark')
    safe_call('/opt/conda/default/bin/jupyter', 'nbextension', 'enable', '--user', '--py', 'jupyter_spark')
    safe_call('/opt/conda/default/bin/jupyter', 'nbextension', 'enable', '--user', '--py', 'widgetsnbextension')

    # create systemd service file for Jupyter notebook server process
    with open('/lib/systemd/system/jupyter.service', 'w') as f:
        opts = [
            '[Unit]',
            'Description=Jupyter Notebook',
            'After=hadoop-yarn-resourcemanager.service',
            '[Service]',
            'Type=simple',
            'User=root',
            'Group=root',
            'WorkingDirectory=/home/hail/',
            'ExecStart=/opt/conda/default/bin/python /opt/conda/default/bin/jupyter notebook --allow-root',
            'Restart=always',
            'RestartSec=1',
            '[Install]',
            'WantedBy=multi-user.target'
        ]
        f.write('\n'.join(opts) + '\n')

    # add Jupyter service to autorun and start it
    safe_call('systemctl', 'daemon-reload')
    safe_call('systemctl', 'enable', 'jupyter')
    safe_call('service', 'jupyter', 'start')
