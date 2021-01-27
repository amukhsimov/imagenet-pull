import setuptools

setuptools.setup(
    name='imagenet_pkg-amukhsimov',
    entry_points={
        'console_scripts': ['imgnet-pull=imagenet_pkg.imagenet_pull:main'],
    },
    version='0.0.1',
    author='Akmal Mukhsimov',
    author_email='aka.mukhsimov@gmail.com',
    packages=setuptools.find_packages(),
    classifiers=[
        'Programmimg Language :: Python :: 3',
        'License :: OSI Approved :: MIT License',
        'Operating System :: Linux Ubuntu 20.04',
    ],
    python_requires='>=3.8',
)
