from setuptools import setup

setup(
  name='sigopt-spark',
  version='1.0.1',
  description='SigOpt Pyspark Integration',
  author='SigOpt',
  author_email='support@sigopt.com',
  url='https://sigopt.com/',
  packages=['sigopt_spark'],
  install_requires = ['sigopt>=5.3.1', 'pyspark>=2.0.0'],
  classifiers=[
    "Development Status :: 5 - Production/Stable",
    "Intended Audience :: Developers",
    "License :: OSI Approved :: Apache Software License",
    "Operating System :: OS Independent",
    "Programming Language :: Python",
    "Topic :: Software Development :: Libraries :: Python Modules",
  ],
)
