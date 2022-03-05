# Guide to Recommending or Adding Datasets
Adding or recommending datasets to be included in OpenPoliceData is a great way to contribute to the project. Help is needed with one or more of the following:
* Identifying previously unknown datasets
* Adding new datasets to OpenPoliceData by adding their information to the ``OpenPoliceData Import Spreadsheet`` and then testing that the data loads properly.
* Same as above but for datasets that require additional coding to allow for loading the new dataset

``Developers``: Please review the [Contributing Guide](https://github.com/sowdm/openpolicedata/blob/sowdm-contributing-file/CONTRIBUTING.MD) (``UPDATE LINK TO MAIN BRANCH!!!``) before or after reviewing this document.

This guide provides suggestions for how to find open police data, what types of data should be included, how to recommend the dataset to the team (if you are recommending), and how to add the dataset (if you are adding). Note that OpenPoliceData does not provide access to all types of police datasets. The types of data that can be added to OpenPoliceData are referred to as qualifying datasets and are described further below.

## What Data Is Already Known About, Requires a Code Update to Add, or Has Already Been Added to OpenPoliceData?
The ``tracking spreadsheet`` (``Add link!!!``) tracks datasets that:
* Have already been added (Added)
* Have been recommended for inclusion but have yet to be added (X)
* Require code updates to add (Code Issue)
* Have been recommended for inclusion but there are reliability or other issues with the download URL or website (URL Issue)

If you would like to add one of these to OpenPoliceData, skip to [How to Add a Dataset](#how-to-add-a-dataset)

The ``tracking spreadsheet`` also includes information on:
* Name of the source (typically a police department)
* State
* Open data website URL (if any)
* Date last checked for open data
* User name of who last checked for open data
* What open data sets are available including if no data is available
* Comments

## How to Find Datasets
First of all, where police departments and other sources share their open data can vary so if you're looking for open data for a specific department, be prepared to search a bit. That being said, there are some general guidelines for finding data in most cases.

### Which Departments/Sources to Check
The [Police Data Initiative](https://www.policedatainitiative.org/datasets) is a good place to start. As of the time of writing this document, not all departments that have qualifying open data have been added from this list at this source.

Otherwise, just select any police department that is either not on the spreadsheet or is on the spreadsheet but has not recently been checked. Then, follow the instructions below for finding data on either their site or their municipality's (the county, city, or town) site.

### Searching for a Department/Source's Data
The most common location of police open data is on the municipality's Open Data site. This is also the location mostly likely to contain qualifying data. The following steps generally work for this case:
* Internet search for ``{town/county/city name} open data``. If it exists, typically, the first link will be the Open Data Portal. Click on it.
![image](https://user-images.githubusercontent.com/67804485/156833142-87bc9c84-11a9-47de-b566-8c7b28c24750.png)
* Often, the data portal has a category called ``Public Safety`` or ``Community Safety``. If so, click on that
* Scan the results for qualifying data sets (See below for more details)

If there is no Open Data Portal or if the Open Data Portal does not contain qualifying police data, data may exist on the department's website. In this case, try searching for ``{town/county/city name} Police Department data`` or going to the department/source's page and looking for a data link.

## What Types of Datasets Can be Included in OpenPoliceData?
There are 3 issues to consider when recommending a dataset to be added:
* Per incident...
* Type of data contained in open data table
* Format of data

## What types of datasets are included and preferred (and not OK) formats?...

Preferred
1.Open Data APIs: ArcGIS or Socrata

Want per-incident data
Not summarized 

Not crime data

Not OK:





## How to notify us of a new dataset or place that is checked?

## How to Add a Dataset

## Creating a new data_loader?
