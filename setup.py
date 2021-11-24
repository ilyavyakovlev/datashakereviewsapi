import setuptools

with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

setuptools.setup(
    name='datashakereviewsapi',
    version=1.0,
    author='Ilya Yakovlev',
    author_email='ilya.v.yakovlev@gmail.com',
    description='Python API to DATASHAKE reviews',
    long_description=long_description,
    long_description_content_type="text/markdown",
    url='https://github.com/ilyavyakovlev/datashakereviewsapi',
    keywords='python api to datashake reviews',
    license='MIT',
    packages=['datashakereviewsapi'],
    install_requires=['requests', 'pandas'],
)
