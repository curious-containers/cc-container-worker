#!/usr/bin/env python3

from distutils.core import setup

setup(
    name='cc-container-worker',
    version='0.12',
    summary='Curious Containers is an application management service that is able to execute thousands of '
            'short-lived applications in a distributed cluster by employing Docker container engines.',
    description='Curious Containers is an application management service that is able to execute thousands of '
                'short-lived applications in a distributed cluster by employing Docker container engines. In this '
                'context applications are atomic entities taking files and parameters as input and producing new files '
                'as output. They are short-lived in a sense that they calculate something and terminate as soon as all '
                'results have been produced. Curious Containers supports scientific use cases like biomedical analysis '
                'and reproducible research by providing standardized methods for packaging applications and executing '
                'them in a compute environment. Therefore application dependencies are added to a compatible Docker '
                'container image, including all necessary bin, binaries and configurations.',
    author='Christoph Jansen, Michael Witt, Maximilian Beier',
    author_email='Christoph.Jansen@htw-berlin.de',
    url='https://github.com/curious-containers/cc-server',
    packages=[
        'cc_container_worker',
        'cc_container_worker.commons',
        'cc_container_worker.application_container',
        'cc_container_worker.data_container',
        'cc_container_worker.inspection_container'
    ],
    entry_points={
        'console_scripts': [
            'cc-application-container=cc_container_worker.application_container.__main__:main',
            'cc-data-container=cc_container_worker.data_container.__main__:main',
            'cc-inspection-container=cc_container_worker.inspection_container.__main__:main'
        ]
    },
    license='Apache-2.0',
    platforms=['any'],
    install_requires=[
        'jsonschema',
        'requests',
        'pymongo',
        'flask',
        'gunicorn',
        'gevent',
        'psutil',
        'paramiko'
    ]
)
