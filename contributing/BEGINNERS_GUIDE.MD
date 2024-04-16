# Beginner's Guide
The Beginner's Guide is our attempt to provide a more detailed description for how to set up your development environment and for helping you get up to speed using Git, GitHub, and Python. It is no doubt imperfect, and [we welcome feedback](https://github.com/openpolicedata/openpolicedata/issues) to help us improve it.

If you run into any trouble don't hesitate to reach out on [GitHub](https://github.com/openpolicedata/opd-data/discussions) or by [email](mailto:openpolicedata@gmail.com).

- [Get Started with GitHub](#get-started-with-github)
- [Git](#git)
- [Conda](#conda)
- [VS Code](#vs-code)
- [Python Tutorials](#python-tutorials)
- [Sphinx Documentation Tutorial](#sphinx-documentation-tutorial)

## Get Started with GitHub
Our project is on GitHub so most contributions will require some interaction with [GitHub](https://github.com/). First, you need to [create an account](https://github.com/join). For learning how to use GitHub, we recommend reading [About GitHub and Git](https://docs.github.com/en/get-started/start-your-journey/about-github-and-git) section from their documentation.

In GitHub, [Issues](https://docs.github.com/en/issues/tracking-your-work-with-issues/about-issues) are used to plan, discuss, and track work. You can [create new issues](https://github.com/openpolicedata/openpolicedata/issues) to suggest changes, bug fixes, and feature enhancements. You can also assign yourself an issue, which indicates that you will work on the issue. A good place to start when looking for an issue to take on are the issues labeled [*Good First Issue*](https://github.com/openpolicedata/openpolicedata/issues?q=is%3Aopen+is%3Aissue+label%3A%22good+first+issue%22), which are meant for developers who are not familiar with OpenPoliceData.

Now, let's create a fork of the OpenPoliceData repository. A [fork](https://docs.github.com/en/pull-requests/collaborating-with-pull-requests/working-with-forks/fork-a-repo) is a copy of a repository where you can make changes independent of the main repository. When you are ready, you can propose that the changes be merged into the main repository by doing what's called a [pull request](https://docs.github.com/en/pull-requests/collaborating-with-pull-requests/proposing-changes-to-your-work-with-pull-requests/creating-a-pull-request) (described later). 
- Go to the [OpenPoliceData repository](https://github.com/openpolicedata/openpolicedata) and click `Fork`
- Click `Create Fork`

Your fork has been created at `https://github.com/{your user name}/openpolicedata`. Now, we need to clone the respository onto your machine so that you can make changes. That is where Git comes in.

## Git
[Git](https://git-scm.com/) is a tool for version control, which enables tracking and merging of changes, the ability to revert to earlier versions of code, and collaboration between developers. Further descriptions and a tutorial can be found [here](https://www.w3schools.com/git/default.asp).

- [Install Git](https://github.com/git-guides/install-git)

On Windows, the installation process has many options. Mostly, the default values are fine. Here are a couple changes that you may want to make:
- **Default editor used by Git**: If it defaults to Vim, you may want to choose something else as Vim is not straightforward to use. There are a variety of other options ([Notepad++](https://notepad-plus-plus.org/), Notepad, Nano, etc.)
- **Adjusting your PATH environment**: Set to `Git from command line and also from 3rd-party software`. This enables Git to be used from PowerShell and the command prompt. Otherwise, you can only use it in Git Bash.

Now, we are ready to clone your OPD fork onto your machine.
- Open a command prompt (perhaps Git Bash if you are on Windows)
- [Generate a new SSH key](https://docs.github.com/en/authentication/connecting-to-github-with-ssh/generating-a-new-ssh-key-and-adding-it-to-the-ssh-agent): An SSH key will allow for secure communication between Git on your local machine and GitHub
- [Add SSH key to your GitHub account](https://docs.github.com/en/authentication/connecting-to-github-with-ssh/adding-a-new-ssh-key-to-your-github-account)
- Navigate in the command prompt to where you want to store the repository. For example:
```
> mkdir repos
> cd repos
```
- Clone your forked repository

`> git clone git@github.com:{Your GitHub Username}/openpolicedata.git`

- Enter the passphase that you created for your SSH key

A folder will be created openpolicedata that contains the contents of your forked repository.

## Conda
Next, you are going to need an installation of Python and an environment manager. [Environment management](https://towardsdatascience.com/getting-started-with-python-environments-using-conda-32e9f2779307) allows you to have multiple sandboxes to work in that are independent of each other. Each environment has their own Python installation and can have different packages/libraries installed in it. Examples of reasons to use an environment manager include:
- You can install only the necessary libraries for your project to ensure that no libraries are called that are not part of your project
- You can have different versions of Python (or Python packages) installed in different environments to allow testing your code in each version

You can download and install Python directly from [Python](https://www.python.org/downloads/). However, we recommend using [Conda](https://docs.conda.io/en/latest/), which provides installation of different Python versions, environment management, and a method for installing difficult-to-install libraries all in one tool.

- Install the minimal conda installation, [Miniconda](https://docs.anaconda.com/free/miniconda/). The default options will be fine. Note: remember the location where you install Miniconda. It will be needed when you configure VS Code below.

Now, let's create the Python environments for OPD. Conda's [Managing environments](https://docs.conda.io/projects/conda/en/latest/user-guide/tasks/manage-environments.html) documentation is a valuable resource to lookup conda commands.  OPD recommends 2 environments, one for development and one for our documentation. You may only want one if you only want to contribute in one area.

### OPD Development Environment
Here's how to create the development environment:
- On Linux, open a command prompt. On Windows, open the Anaconda Powershell Prompt.
- Create a new environment. Below, opd is the name of the environment. It can be anything you like.

`> conda create -n opd python=3.12`

- When prompted to proceed, type y and hit enter.
- Activate the new environment. The name of the new environment should now appear in parentheses in your command prompt.

`> conda activate opd`

- Navigate to the openpolicedata folder.

`> cd {path to parent folder}\openpolicedata`

- Install the libraries that can (easily) be installed with pip

`> pip install -r .\dev\requirements-test.txt`

- Install the libraries that are more easily installed with conda

`> conda install --file .\dev\requirements-conda.txt`

Your OPD development environment is all set.

### OPD Documentation Environment
Now, let's create the OPD documentation environment:
- (Optional) Deactivate the development environment:

`> conda deactivate`

- Create the opd-docs environment and install packages:

`> conda env create -f .\docs\requirements.yml`

- Activate the new environment. The name of the new environment should now appear in parentheses in your command prompt.

`> conda activate opd-docs`

Your OPD documentation environment is all set.

## VS Code
You are going to need some place to do your development, and there are *many* tools for writing and running code. We recommend [VS Code](https://code.visualstudio.com/) because it makes it easy to code, run tests, debug, run Git commands, and use the command line all inside one tool. It is lightweight and has many extensions to make the development process more efficient.

Below, you will our VS Code installation and customization guide along with a few pointers on how to get the most out of VS Code.

### Installation
- [Install VS Code](https://code.visualstudio.com/download).
- Open VS Code
-  [Install the Python extension](https://code.visualstudio.com/docs/python/python-quick-start):
![Alt Text](https://code.visualstudio.com/assets/docs/python/quick-start/qs-python-ext-install.gif)

- Optionally, you can install the Jupyter extension. This will be useful if you want to work on examples or some parts of the documentation.

### Open OpenPoliceData Project Folder
- Go to `File` > `Open Folder`
- Navigate to and select the openpolicedata folder. 

### Conda Configuration
- Go to `View` > `Command Palette` Type `CTRL+SHIFT+P` (or ⇧⌘P on a Mac) to bring up the Command Palette
- *For Windows users*, type `Preferences: Open User Settings (JSON)` and select it in the resulting drop down
- *For Windows users*, add the following inside the curly braces to the `settings.json` file that opens. Location of Miniconda should be where you installed Miniconda above (likely either C:\\ProgramData\\Miniconda3 or C:\\Users\\{Your Windows username}\\miniconda3)
```
"terminal.integrated.defaultProfile.windows": "Anaconda PowerShell",
"terminal.integrated.profiles.windows": {
    "Anaconda PowerShell": {
        "path": "C:\\windows\\System32\\WindowsPowerShell\\v1.0\\powershell.exe",
        "args": ["-ExecutionPolicy", "ByPass", "-NoExit", "-Command", 
            "& '{Location of Miniconda}\\shell\\condabin\\conda-hook.ps1'"]
    }
},
```
- If you don't see the a terminal at the bottom of VS code, go to `View` > `Terminal` to bring it up. 
- *For Windows users*, click the plus (+) sign icon in the terminal panel to create a new terminal (the old terminal was started prior to the settings.json change)
- To test that Conda is now available, run the following in the terminal to output the version of Conda installed:

`> conda --version`

### Set Python Envioronment
- Go to `View` > `Command Palette` Type `CTRL+SHIFT+P` (or ⇧⌘P on a Mac) to bring up the Command Palette
- Type `Python: Select Interpreter` and select it in the resulting drop down
- [Choose a Python environment](https://code.visualstudio.com/docs/python/environments) (i.e. opd or opd-docs). This is the Python environment that will be used for this project in VS Code. You can always change it if you switch from working on development to working on document (or vice versa)

### Debugging Configuration
- Click on the triangle with the bug on the leftmost side to bring up the `Run and Debug` panel
- Click the `create a launch.json file` in the panel that appears to open the `launch.json` file
- Select `Python Debugger`
- Select `Python File Debug the currently active Python file`.  Now, if you select the play button in `Run and Debug` (or type F5), you can [debug](https://code.visualstudio.com/docs/python/debugging) the current file (i.e. whatever Python file is currently open and in view).

### Git in VS Code
- On the leftmost side of VS Code, click on the icon with 3 circles connected by 2 lines bring up the [`Source Control` panel](https://code.visualstudio.com/docs/sourcecontrol/overview). When you make changes to code, you can stage, commit, push, pull, and perform other Git operations using a graphical interface instead of the command line

### Testing Configuration
- Now, click on the icon on the leftmost side that looks like a beaker to open the [`Testing` Panel](https://code.visualstudio.com/docs/python/testing). 
- Click Configure Tests
- Select pytest. OPD uses the pytest framework for testing its code.
- For the directory containing the tests, select tests. Our tests 
are in the .\tests folder.
- The Testing panel should update to look like this:
`> openpolicedata`

- Click the `>` next to `openpolicedata` to expand to see:
```
> openpolicedata
    > tests
```
- Click the `>` next to `tests` to see all the Python files in .\tests that contain our tests.
- Click the `>` next to `test_datasets.py` to see the tests located in .\tests\test_datasets.py. 
- Click the Play button next to `test_duplicates`. This will run the test_duplicates test. This test ensures that there are no datasets listed twice in our dataset source table.
- You may get a window popping up asking you if want to allow public and private networks to access this app. Click Allow. OpenPoliceData needs to be able to access the internet for data including the file containing the dataset source table. If this occurs, notice the red X that now appears in the Testing panel indicating that the test failed (due to not being able to access the source table).
- If the test failed due to the need to permit VS code to access the internet, click the Play button again. You should now see a green check mark indicating that the test passed.

> If the test does't pass, first try [syncing your fork](https://docs.github.com/en/pull-requests/collaborating-with-pull-requests/working-with-forks/syncing-a-fork#syncing-a-fork-branch-from-the-web-ui), running running `git pull`, and trying the test again. If the test still doesn't work, please [submit an issue](https://github.com/openpolicedata/openpolicedata/issues) informing us of this issue.

- Click the folded page/paper with the arrow icon next to `test_duplicates`. This will go to the code for `test_duplicates`.
-  Click to the left of the line number in the `assert` call in the `test_duplicates` function. This will create a breakpoint in `test_duplicates`.
- Click the Play button with the bug in the corner next to `test_duplicates`. This will run `test_duplicates` in debug mode, which will cause the test to stop at your breakpoint.
- In the terminal panel, click `Debug Console`. Here you can examine the parameters being used in the test. This is useful for understanding what went wrong if a test fails. The only variable in `test_duplicates` is `all_datasets` which is a [pandas](https://pandas.pydata.org/docs/user_guide/index.html) DataFrame. It is basically a table containing all the datasets available in OPD.
- Type `all_datasets.head()` in the `Debug Console`. This will print out the first 5 rows of the datasets table.
- Now, click on the Blue Play button in the small debug tools panel (see below). This will cause the test to continue running (it should pass)

![Alt Text](https://code.visualstudio.com/assets/docs/python/debugging/remote-debug-toolbar.png)

### Opening Files
There are 2 main option to open files in VS code. 
- One option is to use the Explorer panel (overlapping pages icon) on the leftmost side of VS Code. This is basically a standard file explorer interface. 
- The other option is to use the Quick Open tool by typing CTRL+P (or ⌘P on a Mac). If you know the partial name of the file that you want to open, you can type it in here and VS code will quickly find it for you.

## Python Tutorials
Here are a couple tutorials that you may find useful for helping to get up to speed with Python:
- [Python's official tutorial](https://docs.python.org/3/tutorial/index.html)
- [First Python notebook](https://palewi.re/docs/first-python-notebook/): a data analysis in Python tutorial with Jupyter notebooks
- [pandas tutorial](https://pandas.pydata.org/docs/user_guide/index.html): pandas is a Python library used extensively in the OpenPoliceData library to handle and analyze tabular data

## Sphinx Documentation Tutorial
We use Sphinx to create our documentation. If you want to help with documentation, we recommend the [Sphinx tutorial](https://www.sphinx-doc.org/en/master/tutorial/index.html).