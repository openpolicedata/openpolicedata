- [ ] Update version number in _version.txt
- [ ] Update dataset dates with update_dates in update_table.py in opd-data repo
- [ ] Test code with min and max Python versions and Ubuntu
- [ ] Review outages
- [ ] Run data cleaning script
- [ ] Update readme with changelog 
- [ ] Review changelog for any necessary documentation updates
- [ ] Update readme and website (datasets/index) latest datasets
- [ ] Update datasets tables in datasets/index
- [ ] Update # of datasets in pyproject.toml, README, index.md, documentation.md, and GitHub about using num_unique in datasets.py
- [ ] Update list of datasets that are modified when loaded by OPD
- [ ] Update number of agencies (README, index.md, documentation.md, pyproject.toml, and datasets/index) using num_sources in datasets module
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
conda create -n v{#}-test python=3.13 -y
conda activate v{#}-test
```
- [ ] Install new version: pip install ..\openpolicedata\dist\openpolicedata-{#}-py3-none-any.whl[optional,test]
```sh
- [ ] Copy tests into a new folder outside openpolicedata folder
- [ ] cd to that folder
- [ ] Run tests: python -m pytest
- [ ] Upload to PyPI
```sh
python -m pip install --upgrade twine
python -m twine upload dist/*
```
- [ ] Update switcher.json file
- [ ] Add version to GitHub and attach whl and .tar.gz files
- [ ] Commit updated sources table
- [ ] Update deprecated data sources table if necessary


To delete env: 
```sh
conda remove --name v{#}-test --all
```