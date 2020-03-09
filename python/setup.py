from setuptools import setup

setup(
  name='sigoptspark',
  version='1.0.0',
  description='SigOpt Pyspark Integration',
  author='SigOpt',
  author_email='support@sigopt.com',
  url='https://sigopt.com/',
  packages=['sigoptspark'],
  install_requires=install_requires,
  install_requires = ['sigopt>=6.0.0', 'pyspark>=2.0.0'],
  classifiers=[
    "Development Status :: 5 - Production/Stable",
    "Intended Audience :: Developers",
    "License :: OSI Approved :: Apache Software License",
    "Operating System :: OS Independent",
    "Programming Language :: Python",
    "Topic :: Software Development :: Libraries :: Python Modules",
  ],
)
