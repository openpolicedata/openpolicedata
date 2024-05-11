- [ ] Update version number in _version.txt
- [ ] Update dataset dates with update_dates in update_table.py in opd-data repo
- [ ] Test code with min and max Python versions and Ubuntu
- [ ] Run data cleaning script
- [ ] Update readme with changelog 
- [ ] Update readme and website (datasets/index) latest datasets
- [ ] Update datasets tables in datasets/index
- [ ] Update # of datasets in pyproject.toml, README, index.md, documentation.md, and GitHub about using num_unique in datasets.py
- [ ] Update list of datasets that are modified when loaded by OPD
- [ ] Update number of agencies (README, index.md, documentation.md, pyproject.toml, and datasets/index) using count_agencies in update_table.py in opd-data repo
- [ ] Rerun jupyter notebooks as needed
- [ ] Update map image using gen_sources_map.ipynb in opd-date repo
- [ ] Update version number and date in citations/index.md
- [ ] Build the new version
```sh
python -m pip install --upgrade pip
python -m pip install --upgrade build
python -m build
```
- [ ] Create and activate a new environment: 
```sh
conda create -n v{#}-test python=3.12
```
- [ ] Install new version
```sh
python -m pip install .\dist\openpolicedata-{version}-py3-none-any.whl[optional,test]
- [ ] Copy tests into a new folder outside openpolicedata folder
- [ ] cd to that folder
- [ ] Run tests: python -m pytest
- [ ] Upload to PyPI
```sh
python -m pip install --upgrade twine
python -m twine upload dist/*
```
- [ ] Add version to GitHub and attach whl and .tar.gz files
- [ ] Commit updated sources table
- [ ] Update switcher.json file


To delete env: 
```sh
conda remove --name v{#}-test --all
```