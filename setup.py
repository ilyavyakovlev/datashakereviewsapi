import setuptools

with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

setuptools.setup(
    name='datashakereviewsapi',
    version=1.01,
    author='Ilya Yakovlev',
    author_email='ilya.v.yakovlev@gmail.com',
    description='Python API to DATASHAKE reviews',
    long_description=long_description,
    long_description_content_type="text/markdown",
    url='https://github.com/ilyavyakovlev/datashakereviewsapi',
    keywords='python api to datashake reviews',
    license='BSD-3',
    packages=['datashakereviewsapi'],
    install_requires=['requests', 'pandas'],
)
