# Contributors Guide

Based on the guide from: https://github.com/Unidata/MetPy

Interested in helping build OpenPoliceData? Have code from your research that you believe others will find useful? Have a few minutes to tackle an issue or find new sources of police data? In this guide, we will get you setup and integrated into contributing to OpenPoliceData!

## Introduction

First off, thank you for considering contributing to OpenPoliceData. OpenPoliceData is community-driven project. It's people like you that make OpenPoliceData useful and successful. There are many ways to contribute including, but not limited to, writing tutorials or examples, improvements to the documentation, submitting bug reports and feature requests, writing code to fix an issue or add a feature which can be incorporated into OpenPoliceData for everyone to use, and simply just informing us of a data set that is not already available in OpenPoliceData.

Following these guidelines helps to communicate that you respect the time of the developers managing and developing this open source project. In return, they should reciprocate that respect in addressing your issue, assessing changes, and helping you finalize your pull requests.

So, please take a few minutes to read through this guide and get setup for success with your OpenPoliceData contributions. We're glad you're here!

## What Can I Do?

* Whether you know how to code or not, you can help us find and add new data sets to OpenPoliceData. See [Adding Datasets Guide](https://github.com/sowdm/openpolicedata/blob/sowdm-contributing-file/dev/ADDING_DATASETS.MD) for more details. (``TODO: Convert the Adding Datasets link to the main branch!!!``)
* Tackle any [issues](https://github.com/sowdm/openpolicedata/issues) you wish! We have a special label for issues that beginners might want to try. Have a look at our [current beginner issues](https://github.com/sowdm/openpolicedata/labels/good%20first%20issue). Also have a look at if the issue is already assigned to someone - this helps us make sure that work is not duplicated.
* Contribute code you already have. It does not need to be perfect! We will help you clean things up, test it, etc.
* Make a tutorial or example of how to do something.
* Improve documentation of a feature you found troublesome.
* File a new issue if you run into problems!

## Ground Rules

The goal is to maintain a diverse community that's pleasant for everyone. Please be considerate and respectful of others by following our
[code of conduct](https://github.com/sowdm/openpolicedata/blob/main/CODE_OF_CONDUCT.md).

Other items:

* Each pull request should consist of a logical collection of changes. You can include multiple bug fixes in a single pull request, but they should be related. For unrelated changes, please submit multiple pull requests.
* Do not commit changes to files that are irrelevant to your feature or bug fix (eg: .gitignore).
* Be willing to accept criticism and work on improving your code; we don't want to break other users' code, so care must be taken not to introduce bugs.
* Be aware that the pull request review process is not immediate, and is generally proportional to the size of the pull request.

## Reporting a bug

The easiest way to get involved is to report issues you encounter when using OpenPoliceData or by requesting something you think is missing.

* Head over to the [issues](https://github.com/sowdm/openpolicedata/issues) page.
* Search to see if your issue already exists or has even been solved previously.
* If you indeed have a new issue or request, click the "New Issue" button and provide as much information as possible on your issue including OS and version information.

## Setting up your development environment

We recommend using the [conda](https://conda.io/docs/) package manager for your Python environments. This is particularly true in Windows where the required [geopandas](https://geopandas.org/en/stable/getting_started/install.html) package is more difficult to install without conda.
This requires some comfort with the command line and a little ``git`` knowledge.
Our recommended setup for contributing:

Install [miniconda](https://docs.conda.io/en/latest/miniconda.html) on your system.
You may have to restart your prompt for the remaining steps to work.

Install [git](https://git-scm.com/downloads)
(link with instructions) on your system if not already available
(check with ``git --version`` at the command line).
This can also be installed from a variety of package managers, including ``conda`` if needed.

Login to your [GitHub](https://github.com) account and make a fork of the
[OpenPoliceData repository](https://github.com/sowdm/openpolicedata/) by clicking the "Fork" button.
Clone your fork of the OpenPoliceData repository (in terminal on Mac/Linux or git shell/GUI on Windows) to the location you'd like to keep it.
We are partial to creating a ``git_repos`` or ``projects`` directory in our home folder.

```sh
git clone https://github.com/<your-user-name>/openpolicedata.git
```

Navigate to that folder in the terminal or in Anaconda Prompt if you're on Windows.
The remainder of the instructions will take place within this directory.

```sh
cd openpolicedata
```

Connect your repository to the upstream (main project).

```sh
git remote add sowdm https://github.com/sowdm/openpolicedata.git
```

Create a new conda environment, and give it a name.
After ``-n`` you can specify any name you'd like; here we've chosen ``devel``.

```sh
conda create -n devel
```

**IMPORTANT**: Always activate this environment when developing and testing your changes!

```sh
conda activate devel
```

You will have to do this any time you re-open your prompt.
Currently there are no packages in this environment, let's change that.

Install the necessary dependency packages that are available on conda.
Remember that these must be executed within the ``openpolicedata`` directory.

```sh
conda install --file conda/requirements-conda.txt --file dev/requirements-test.txt
```

Install the necessary dependency packages that are NOT available on conda.
Remember that these must be executed within the ``openpolicedata`` directory.

```sh
pip install -r conda/requirements-pip.txt
```

Now you're all set!
You have an environment called ``devel`` that you can work in.
Remember, you will need to activate this environment the
next time you want to use it after closing the terminal.
If you want to get back to the root environment, run ``conda deactivate``.

### API Keys and Application Tokens
Some REST APIs used by OpenPoliceData either either require an API key / app token or have the option of throttling (slowing) your request without an API key / app token.

In order for OpenPoliceData to access the API key or app token, environment variables are used.
* Instructions for setting environment variables on [Windows](https://www.wikihow.com/Create-an-Environment-Variable-in-Windows-10)
* Instructions for setting environment variables on [Linux](https://phoenixnap.com/kb/linux-set-environment-variable)

Data accessed from [Socrata](https://dev.socrata.com/docs/app-tokens.html) may throttle your requests if you make too many. The following instructions describe how to add a Socrata app token:
* [Generate an app token](https://support.socrata.com/hc/en-us/articles/210138558-Generating-an-App-Token)
* Copy the App Token in the App Token column of the table at [evergreen.data.socrata.com](evergreen.data.socrata.com)
* Add an environment variable called SODAPY_API_KEY and set the value to the Socrata App Token

## Pull Requests

The changes to the OpenPoliceData source (and documentation) should be made via GitHub pull requests against ``main``, even for those with administration rights. While it's tempting to make changes directly to ``main`` and push them up, it is better to make a pull request so
that others can give feedback. 

During the Pull Request process, before the final merge, it's a good idea to rebase the branch and squash together smaller commits. It's not necessary to flatten the entire branch, but it can be nice to eliminate small fixes and get the merge down to logically arranged commits. This
can also be used to hide sins from history--this is the only chance, since once it hits ``main``, it's there forever!

**Working on your first Pull Request?** You can learn how from this *free* video series
[How to Contribute to an Open Source Project on GitHub](https://egghead.io/courses/how-to-contribute-to-an-open-source-project-on-github),
Aaron Meurer's [tutorial on the git workflow](https://www.asmeurer.com/git-workflow/), or the
guide [“How to Contribute to Open Source"](https://opensource.guide/how-to-contribute/).

Commit the changes you made. Chris Beams has written a [guide](https://cbea.ms/git-commit/) on how to write good commit messages.

Push to your fork and [submit a pull request](https://github.com/sowdm/openpolicedata/compare).

## Source Code

OpenPoliceData's source code is located in the `openpolicedata/` directory in the root of the repository. There are currently 3 main components to OpenPoliceData:

* `_datasets.py`: Constructs the table of available datasets and provides query capabilities on those datasets.
* `data.py`: Contains the classes for loading and saving datasets.
* `data_loaders.py`: Contains functions for downloading data from URLs.

## Documentation

TBD. Perhaps you can help us get our documentation site started!

## Tests

Unit tests are the lifeblood of the project, as it ensures that we can continue to add and change the code and stay confident that things have not broken. Running the tests requires ``pytest``, which is easily available through ``conda`` or ``pip``. It was also installed if you made our default ``devel`` environment.

### Running Tests

Running the tests can be done by running ``python -m pytest`` in the project root directory. This runs most of the tests except a few slow ones. Prior to a major commit, it is advised to run all the tests:

```sh
python -m pytest --runslow
```

### Writing Tests

Tests should ideally hit all of the lines of code added or changed. 

Let's say we are adding a simple function to add two numbers and return the result as a float or as a string. (This would be a silly function, but go with us here for demonstration purposes.)

```python
def add_as_float_or_string(a, b, as_string=False):
    res = a + b
    if as_string:
       return string(res)
    return res
```

I can see two easy tests here: one for the results as a float and one for the results as a string. If I had added this to the ``data`` module, I'd add those two tests in ``tests/test_data.py``.

```python
def test_add_as_float_or_string_defaults():
    res = add_as_float_or_string(3, 4)
    assert(res, 7)


def test_add_as_float_or_string_string_return():
    res = add_as_float_or_string(3, 4, as_string=True)
    assert(res, '7')
```

## Code Style

We try to conform to good Python style (i.e. [PEP8](https://pep8.org)). However, we are not experts in it, and don't expect you to be. Try your best to conform to the general style used in the existing code.

Also, if you want to make the OpenPoliceData code conform to PEP8 better, the help would be greatly appreciated!

## What happens after the pull request

You've make your changes, documented them, added some tests, and submitted a pull request. What now?

### Code Review

At this point you're waiting on us. You should expect to hear at least a comment within a couple of days. We may suggest some changes or improvements or alternatives.

Some things that will increase the chance that your pull request is accepted quickly:

* Write tests.
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
``TODO: If we turn on the Discussions board option in the GitHub settings, we could tell them to ask questions there ``
