## Usage

### To test download.py module use:

```bash
python3 download.py
```

Output:
```
...
DATASET for regions: ['STC', 'MSK', 'PAK']:
...0.   <Region> --- <['STC' 'STC' 'STC' ... 'PAK' 'PAK' 'PAK']> --- "<U3"
...1.   <ID> --- <['002100160187' '002100160269' '002100160763' ... '171106200811'
 '171106200812' '171106200813']> --- "<U12"
...2.   <Communication type> --- <[0 0 0 ... 3 6 6]> --- "int8"
...
```

### To use get_stat.py module use:

To show figure with all regions and save it into the `data/figures/` with name and format `test1.png`

```bash
python3 get_stat.py --show_figure --fig_location data/figures/test1.png
```

or

```bash
python3 get_stat.py -s -l data/figures/test2.png
```

To show figure with specific regions and save it into the `data/figures/` with name and format `test2.png`

```bash
python3 get_stat.py --region PHA KVK JHM --show_figure --fig_location data/figures/test3.png
```

or

```bash
python3 get_stat.py -r PHA KVK JHM  -s -l data/figures/test4.png
```