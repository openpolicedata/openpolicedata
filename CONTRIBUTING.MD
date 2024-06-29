# Contributors Guide

Based on the guide from: https://github.com/Unidata/MetPy

Interested in helping build OpenPoliceData (OPD)? Have code from your research that you believe others will find useful? Have a few minutes to tackle an issue or find new sources of police data? In this guide, we will get you setup and integrated into contributing to OPD!

- [Introduction](#introduction)
- [What Can I Do?](#what-can-i-do)
- [Ground Rules](#ground-rules)
- [Reporting a Bug](#reporting-a-bug)
- [Beginner's Guide](#beginners-guide)
- [Setting Up Your Development Environment](#setting-up-your-development-environment)
- [Pull Requests](#pull-requests)
- [Source Code](#source-code)
- [Documentation](#documentation)
- [Tests](#tests)
- [Code Style](#code-style)
- [What Happens After the Pull Request](#what-happens-after-the-pull-request)
- [Further Reading](#further-reading)
- [Still Have Questions?](#still-have-questions)

## Introduction

First off, thank you for considering contributing to OpenPoliceData. OPD is community-driven project. It's people like you that make OPD useful and successful. 

There are many ways to contribute. Some suggestions are provided below. We welcome contributions from people of all backgrounds and skill levels, and we are happy to help find an area that is a good match for your interests and skills.

Following these guidelines helps to communicate that you respect the time of the developers managing and developing this open source project. In return, they should reciprocate that respect in addressing your issue, assessing changes, and helping you finalize your pull requests.

Please take a few minutes to read through this guide and get setup for success with your OpenPoliceData contributions. We're glad you're here!

## What Can I Do?
There are a variety of ways you can contribute. Here are some options, but don't feel limited to these.

### No Python Required
Whether you know how to code or not, you can help us by finding and/or adding new datasets to OPD. If you would like to help us with this, skip to our [Adding Datasets Guide](https://github.com/openpolicedata/opd-data/blob/main/CONTRIBUTING.MD) for more details. 

### Some Python Required
These options are meant for Python beginners or those looking to contribute while becoming familiar with using OPD. 

NOTE: If you're new to Python or GitHub (or Sphinx for our documentation), our [Beginner's Guide](https://github.com/openpolicedata/openpolicedata/blob/main/contributing/BEGINNERS_GUIDE.MD) will help get you set up.
- Try out OPD and suggest improvements or new features to the code or [documentation](https://openpolicedata.readthedocs.io/en/stable) by [submitting an issue](https://github.com/openpolicedata/openpolicedata/issues). Check out our [getting started guide](https://openpolicedata.readthedocs.io/en/stable/getting_started/index.html) and/or [examples](https://openpolicedata.readthedocs.io/en/stable/examples/index.html).
- Improve documentation of a feature you found confusing or provide feedback on our documentation by [submitting a documentation issue](https://github.com/openpolicedata/openpolicedata/issues?q=is%3Aopen+is%3Aissue+label%3Adocumentation).
- Make a tutorial or example of how to do something
- Create a [bug issue](https://github.com/openpolicedata/openpolicedata/issues?q=is%3Aopen+is%3Aissue+label%3Abug) for problems you have using our code

### Python Required
These options require at least some understanding of Python.
- Add a dataset to our source table and run our tests to ensure that OPD is able to load it properly. This may be one that you found or a [pre-existing issue](https://github.com/openpolicedata/openpolicedata/issues?q=is%3Aopen+is%3Aissue+label%3Adata). See our [Adding Datasets Guide](https://github.com/openpolicedata/opd-data/blob/main/CONTRIBUTING.MD).
- Take on an [issues](https://github.com/openpolicedata/openpolicedata/issues) (possibly one you submitted in one of the above tasks!). We have a special label for issues that people new to OPD might want to try. Have a look at our [good first issues](https://github.com/openpolicedata/openpolicedata/labels/good%20first%20issue). These good first issues are for a range of skill levels not just beginners. What ties them together is they don't require a strong working knowledge of the structure of the OPD code. If you are interested in an issue, please check if it is already assigned to someone - this helps us make sure that work is not duplicated.
- Contribute code you already have. It does not need to be perfect! We will help you clean things up, test it, etc.
- Write a tutorial or examples of OPD usage

If you're not sure how you want to contribute reach out and ask on [GitHub](https://github.com/openpolicedata/opd-data/discussions) or by [email](mailto:openpolicedata@gmail.com).

## Ground Rules

The goal is to maintain a diverse community that's pleasant for everyone. Please be considerate and respectful of others by following our
[code of conduct](https://github.com/openpolicedata/openpolicedata/blob/main/CODE_OF_CONDUCT.md).

Other items:

* Each [pull request](https://docs.github.com/en/pull-requests/collaborating-with-pull-requests/proposing-changes-to-your-work-with-pull-requests/about-pull-requests) should consist of a logical collection of changes. You can include multiple bug fixes in a single pull request, but they should be related. For unrelated changes, please submit multiple pull requests.
* Do not commit changes to files that are irrelevant to your feature or bug fix (eg: .gitignore).
* Be willing to accept criticism and work on improving your code; we don't want to break other users' code, so care must be taken not to introduce bugs.
* Be aware that the pull request review process is not immediate, and is generally proportional to the size of the pull request.

## Reporting a Bug

The easiest way to get involved is to report issues you encounter when using OpenPoliceData or reading our documentation or by requesting something you think is missing.

* Head over to the [issues](https://github.com/openpolicedata/openpolicedata/issues) page.
* Search to see if your issue already exists or has even been solved previously.
* If it doesn't exist, click the "New Issue" button and provide as much information as possible on your issue.

## Beginner's Guide
We welcome contributions from people new to open source, Git, GitHub, Python, and programming. We have put together a [Beginner's Guide](https://github.com/openpolicedata/openpolicedata/blob/main/contributing/BEGINNERS_GUIDE.MD) to help you get up to speed as quickly as possible.

## Setting Up Your Development Environment
We have split environment setup into 2 sections: Python and OpenPoliceData. There are many ways to set up your Python environment. We provide setup instructions for our preferred tools, but you are welcome to use your own. The OpenPoliceData section is specific to OPD: what needs to be installed to develop OPD. If you find setting up your environment challenging, the [Beginner's Guide](#beginners-guide) section may help.

### Python Environment Setup
We are defining the Python environment to consist of GitHub, Git, your code editor, and your environment manager.

Our preferred code editor (of the *many* options) is [VS Code](https://code.visualstudio.com/), which is lightweight and provides extensions to add many customizations. The [quick start guide](https://code.visualstudio.com/docs/python/python-quick-start) will get you started and provides next steps for debugging, testing, etc. in VS Code. You are obviously free to you use your own preferred tool(s).

We recommend using [conda](https://conda.io/docs/) package manager for your Python environments. On Windows, installing the *optional* [geopandas](https://geopandas.org/en/stable/getting_started/install.html) package is easier with conda.

The following instructions requires some comfort with the command line and a little ``git`` knowledge. Our recommended setup for contributing:

- Install [miniconda](https://docs.conda.io/en/latest/miniconda.html) on your system.
- Install [git](https://git-scm.com/downloads) on your system if not already available
- Open a command prompt (terminal on Mac/Linux or Git Bash on Windows)
- Run `git --version` to confirm installation

```sh
> git --version
```

### OpenPoliceData Setup
Below is the setup specific to OpenPoliceData. Instructions are included for setting up 2 environments: 

1. A development environment *required for code that is part of the OpenPoliceData library*
2. A documentation environment *required for building our Sphinx documentation*

You are free to install one or both environments depending on how you want to contribute.

- Login to your [GitHub](https://github.com) account and make a fork of the
[OpenPoliceData repository](https://github.com/openpolicedata/openpolicedata/) by clicking the "Fork" button
- Clone your fork of the OpenPoliceData repository (in terminal on Mac/Linux or Git Bash on Windows) to the location you'd like to keep it. We are typically create a ``repos`` or ``projects`` directory in our home folder. `--recursive` enables cloning of the opd-examples submodule used in the documentation.

```sh
> git clone --recursive git@github.com:{Your GitHub Username}/openpolicedata.git
```

- Navigate to the openpolicedata folder in the terminal or in Anaconda Prompt if you're on Windows. The remainder of the instructions will take place within this directory.

```sh
> cd openpolicedata
```

- Connect your repository to the upstream (main project).

```sh
git remote add upstream https://github.com/openpolicedata/openpolicedata.git
```

#### Development Environment

- Create a new conda environment, and give it a name. After ``-n`` you can specify any name you'd like; here we've chosen ``opd``.

```sh
> conda create -n opd python=3.12  # Python version can be changed to your preferred version
```

- **IMPORTANT**: Always activate this environment when developing and testing code changes!

```sh
> conda activate opd
```

You will have to do this any time you re-open your prompt.
Currently there are no packages in this environment, let's change that.

- Install required packages. Remember that these must be executed within the base ``openpolicedata`` directory.

```sh
> pip install -r .\dev\requirements-test.txt
```

- Install the libraries that are more easily installed (on Windows at least) with conda. 

```sh
> conda install -c conda-forge  --file .\dev\requirements-conda.txt
```

Now you're all set!
You have an environment called ``opd`` that you can work in.
Remember, you will need to activate this environment the
next time you want to use it after closing the terminal.
If you want to get back to the root environment, run ``conda deactivate``.

#### Documentation Environment

- Create the opd-docs environment and install packages. The `requirements.yml` file contains the name of the environement and requirements for the documentation environment

```sh
> conda env create -f .\docs\requirements.yml
```

- **IMPORTANT**: Always activate this environment when building documentation!

```sh
> conda activate opd-docs
```

Now you're all set!
You have an environment called ``opd-docs`` that you can work in.
Remember, you will need to activate this environment the
next time you want to use it after closing the terminal.
If you want to get back to the root environment, run ``conda deactivate``.

### API Keys and Application Tokens
Some REST APIs used by OpenPoliceData either require an API key / app token or have the option of throttling (slowing) your request without an API key / app token.

Data accessed from [Socrata](https://dev.socrata.com/docs/app-tokens.html) may throttle your requests if you make too many. The following instructions describe how to add a Socrata app token:
* [Generate an app token](https://support.socrata.com/hc/en-us/articles/210138558-Generating-an-App-Token)
* Copy the App Token in the App Token column of the table at [evergreen.data.socrata.com](https://evergreen.data.socrata.com/)
* Add an environment variable called `SODAPY_API_KEY` and set the value to the Socrata App Token. `SODAPY_API_KEY` is the expected name for the Socrata API key in the OpenPoliceData code.
    * Instructions for setting environment variables on [Windows](https://www.wikihow.com/Create-an-Environment-Variable-in-Windows-10)
    * Instructions for setting environment variables on [Linux](https://phoenixnap.com/kb/linux-set-environment-variable)


## Pull Requests

The changes to the OpenPoliceData source code and documentation should be made via GitHub pull requests against ``main``, even for those with administration rights. While it's tempting to make changes directly to ``main`` and push them up, it is better to make a pull request so
that others can give feedback. 

During the Pull Request process, before the final merge, it's a good idea (but not required) to rebase the branch and squash together smaller commits. It's not necessary to flatten the entire branch, but it can be nice to eliminate small fixes and get the merge down to logically arranged commits. This
can also be used to hide sins from history--this is the only chance, since once it hits ``main``, it's there forever!

**Working on Your First Pull Request?** You can learn how from this *free* video series
[How to Contribute to an Open Source Project on GitHub](https://egghead.io/courses/how-to-contribute-to-an-open-source-project-on-github),
Aaron Meurer's [tutorial on the git workflow](https://www.asmeurer.com/git-workflow/), or the
guide [“How to Contribute to Open Source"](https://opensource.guide/how-to-contribute/).

Commit the changes you made. Chris Beams has written a [guide](https://cbea.ms/git-commit/) on how to write good commit messages.

Push to your fork and [submit a pull request](https://github.com/openpolicedata/openpolicedata/compare).

## Source Code

OpenPoliceData's source code is located in the `openpolicedata/` directory in the root of the repository (i.e. in `openpolicedata/openpolicedata`). There are currently 4 main components to OpenPoliceData:

* `datasets.py`: Constructs the table of available datasets and provides query capabilities on those datasets.
* `data.py`: Contains the classes for loading and saving datasets.
* `data_loaders.py`: Contains (currently) internal capabilities used by `data.py` for downloading data from URLs.
* `preproc.py`: Contains (currently) internal capabilities used by `data.py` for standardizing loaded data.

Check out the [Getting Started Guide](https://openpolicedata.readthedocs.io/en/stable/getting_started/index.html) and [Examples](https://openpolicedata.readthedocs.io/en/stable/examples/index.html) documentation for examples of how to use the OPD code.

## Documentation

The OPD documentation is in the `docs` folder in the root project directory. OPD uses [Sphinx](https://www.sphinx-doc.org/en/master/) and extensions of Sphinx to develop documentation. These tools are installed when you install the [`opd-docs` environment](#documentation-environment). 

If you are not familiar with Sphinx, their [tutorial](https://www.sphinx-doc.org/en/master/tutorial/index.html) is a good place to start.

[`Read the Docs`](https://readthedocs.org/) is used to convert our documentation code in the `docs` folder into the [OPD website](https://openpolicedata.readthedocs.io/en/stable/). To get started, we recommend their [tutorial](https://docs.readthedocs.io/en/stable/tutorial/index.html#getting-started).

Improving our documentation is a good place to make your first contribution (even if you ultimately want to contribute code). While learning how OPD works from the [Getting Started Guide](https://openpolicedata.readthedocs.io/en/stable/getting_started/index.html) or checking out the [examples](https://openpolicedata.readthedocs.io/en/stable/examples/index.html), you might find typos, sentences that could be phrased better, concepts that could be explained better, or code that does not currently work . Feel free to create an issue for the recommended change (no matter how small). You can then likely make the update yourself as documentation updates are pretty straightforward to make. This is a good and simple way to make your first pull request with OPD.

## Tests
Tests are the lifeblood of this project as our users need to be confident that they are receiving the data that they requested. Errors will result in flawed data analysis.

We use [`pytest`](https://docs.pytest.org/en/8.2.x/), which is easily available through ``conda`` or ``pip``. It was installed if you made the above [``opd`` development environment](#development-environment). If you have not used pytest, see their ['Getting Started Guide](https://docs.pytest.org/en/8.2.x/getting-started.html). You may also find [VS Code's Python testing tools](https://code.visualstudio.com/docs/python/testing) useful for easily running and debugging tests.

> NOTE: Running all of OPD's tests takes a long time. The full suite of tests runs tests on all of our datasets in order to identify datasets that are no longer available (or temparily unavailable) or have moving and to ensure that our code works on all datasets. Due to the sheer number of datasets available in OPD, there is always a decent chance that some of them are temporarily unavailable or are no longer available (either because the link has changed or they have been taken down). We try to throw warnings rather than errors in our tests when OPD throws an error due to a dataset that is not available so that you can better test your specific changes. However, we are not always 100% successful at that. Please [reach out](#still-have-questions) if you have any questions whether your test is failing because of a bug in the code or a dataset that needs fixed (on our end or on the data providers).

What follows is a description of how to run OPD's tests from the command line and in VS code. If using the command line, it is assumed that you are in the root folder of openpolicedata directory.

### Run all tests
```sh
> python -m pytest
```

In VS Code: click the Play button next to openpolicedata in the Testing module (beaker icon on far left).

### Run all tests from a single file
Our full suite of tests can take a long time to run. It may be possible to fully test your code by running tests from one or more of the individual test files (test_defs.py in this example). 
```sh
> python -m pytest ./tests/test_defs.py
```

In VS Code: click the Play button next to test_defs.py in the Testing module (beaker icon on far left).

### Run a single test
Running a single test in a file is not possible from the command line.

In VS Code: click the Play button next to the desired test in the Testing module (beaker icon on far left).

### Running tests with command line arguments
There are both [standard](https://docs.pytest.org/en/7.1.x/reference/reference.html#command-line-flags) and OPD custom command line arguments that can be passed to pytest. The OPD custom arguments are defined in the `pytest_addoption` function in [`.\tests\conftest.py`](https://github.com/openpolicedata/openpolicedata/blob/main/tests/conftest.py).

When running tests in VS Code, command line arguments can be defined in the `pyproject.toml` file in the root directory of the openpolicedata project as described [here](https://docs.pytest.org/en/7.1.x/reference/customize.html#pyproject-toml). For example, the following adds -s (standard pytest) and --csvfile, --runslow, and --skip (OPD custom) inputs to all pytest calls in VS Code:

`addopts = "-s --csvfile='..\\opd-data\\opd_source_table.csv' --runslow --skip 'Sacramento,Beloit'"`

Of the [standard  pytest command line arguments](https://docs.pytest.org/en/7.1.x/reference/reference.html#command-line-flags), `-x` to stop after the first test failure and `-s` to not capture print statements by the code (pytest defaults to suppressing print statements) have been found to be most useful.

The OPD custom pytest arguments are described below:

* `--runslow`: There are some tests are marked to only run when this flag is present. 
* `--csvfile`: Follow this flag with the filename of a local CSV file. When set, datasets located in a local CSV file will be used for testing rather than ones from the [standard datasets file](https://github.com/openpolicedata/opd-data/blob/main/opd_source_table.csv). This is useful when datasets have been added or edited, and you want to test them prior to submitting pull requests for them. See the [Adding Datasets Contributing Guide](https://github.com/openpolicedata/opd-data/blob/main/CONTRIBUTING.MD) for more details on adding datasets.

> NOTE: In our experience, (for unknown reasons) this input must be entered as `--csvfile='/path/to/opd_source_table.csv'` rather than the typical `--csvfile /path/to/opd_source_table.csv` or `--csvfile '/path/to/opd_source_table.csv'`.

* `--use-changed-rows`: This input only runs datasets through the tests that have been changed or editted based on the output of `git diff` on a local CSV file. Currently, this will only run on files located in `../opd-data/opd_sources_table.csv` (i.e. the [opd-data](https://github.com/openpolicedata/opd-data) repository is located in the same folder containing the root openpolicedata folder). Similar to `--csvfile`, this input is useful when adding and editing datasets. `--use-changed-rows` greatly speeds up testing when dataset changes are being tested (not OPD code changes). See the [Adding Datasets Contributing Guide](https://github.com/openpolicedata/opd-data/blob/main/CONTRIBUTING.MD) for more details on adding datasets.
* `--source`: Follow with the name of a single source whose datasets will be tested, such as `--source Chicago`. This input can be useful when adding or editing datasets.
* `--skip`: Follow with a comma-separated list of sources to skip, such as `--skip 'Sacramento,Beloit` to skip datasets from Sacramento and Beloit. This can be useful when certain datasets are unavailable and cause some tests to fail due to issues with the datsets themselves, not OPD's code.
* `--start`: Follow with the index of a dataset. Tests that arun on all datasets will skip datasets prior to this index. This can be useful when restarting a long tests that loops over all the datasets (perhaps if the failure was due to an issue with the dataset, not OPD). 

### Writing New Tests

Tests should ideally hit all of the lines of code added or changed. 

Let's say we are adding a simple function to add two numbers and return the result as a float or as a string. (This would be a silly function, but go with us here for demonstration purposes.)

```python
def add_as_float_or_string(a, b, as_string=False):
    res = a + b
    if as_string:
       return string(res)
    return res
```

I can see two easy tests here: one for the results as a float and one for the results as a string. If I had added this to the `datasets.` module, I might add these two tests in ``tests/test_datasets.py``.

```python
def test_add_as_float_or_string_defaults():
    res = add_as_float_or_string(3, 4)
    assert(res, 7)


def test_add_as_float_or_string_string_return():
    res = add_as_float_or_string(3, 4, as_string=True)
    assert(res, '7')
```

## Code Style

We try to conform to good Python style (i.e. [PEP8](https://pep8.org)). However, we are not experts in it and don't expect you to be. Try your best to conform to the general style used in the existing code.

Also, if you want to make the OpenPoliceData code conform to PEP8 better, the help would be greatly appreciated!

## What Happens After the Pull Request

You've made your changes, documented them, added some tests, and submitted a pull request. What now?

### Code Review

At this point you're waiting on us. You should expect to hear at least a comment within a couple of days. We may suggest some changes or improvements or alternatives.

Some things that will increase the chance that your pull request is accepted quickly:

* Write tests if change is not covered by current tests.
* Be consistent with the overall style of the code.
* Write a [good commit message](https://tbaggery.com/2008/04/19/a-note-about-git-commit-messages.html).


### Merging

Once we're all happy with the pull request, it's time for it to get merged in. Only the maintainers can merge pull requests and you should never merge a pull request you have commits on as it circumvents the code review. If this is your first or second pull request, we'll likely help by rebasing and cleaning up the commit history for you. As your development skills
increase, we'll help you learn how to do this.

## Further Reading

There are a ton of great resources out there on contributing to open source and on the
importance of writing tested and maintainable software.

* [How to Contribute to Open Source Guide](https://opensource.guide/how-to-contribute/)
* [Zen of Scientific Software Maintenance](https://jrleeman.github.io/ScientificSoftwareMaintenance/)

## Still Have Questions?
Check out our [Discussions page](https://github.com/openpolicedata/openpolicedata/discussions) or [send us an email](mailto:openpolicedata@gmail.com)
