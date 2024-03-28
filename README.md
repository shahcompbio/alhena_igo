# alhena_igo

Alhena loader for IGO.

This extends the existing alhenaloader module by providing methods that'll pull data as needed from Isabl to load.


## Install


```
git clone https://github.com/shahcompbio/alhena_igo.git

conda create -n alhena python=3.9

source alhena

cd alhena_igo

make build
```

## Commands

To load a single analysis:

```
alhena_igo --host <host> load --id <aliquot ID> --project SPECTRUM --project DLP
```



To load an entire analysis over from Isabl

```
alhena_igo --host <host> load-project --alhena <project name in Alhena> --isabl <Project ID in Isabl>
```

Note that the above requires the project name to already exist in Alhena. If this is not the case, then run:

```
alhenaloader --host <host> add-project <name of project in Alhena>
```


To remove an analysis

```
alhena_igo --host <host> clean --id <aliquot ID>

or

alhena_igo --host <host> clean --analysis <Alhena ID>
```


## Authors

* **Samantha Leung** - *Initial work* - [github](https://github.com/redpanda_cat)

See also the list of [contributors](https://github.com/redpanda_cat/alhena_igo/contributors) who participated in this project.

## LicenseMIT License

Copyright (c) redpanda_cat

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
