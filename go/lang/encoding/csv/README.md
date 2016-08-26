Noms' fork of Go's encoding/csv package deals in byte slices, not string arrays, to avoid allocation.

For example, given a CSV:
```
this,is,a,CSV
here,is,another,row
```
Go will produce:
```
[]string{"this", "is", "a", "csv"}
[]string{"this", "is", "another", "row"}
```
Noms will produce tuples `[]byte, [][]int`:
```
[]byte{"this,is,a,CSV"}, [][]int{{0, 4}, {5, 7}, {8, 9}, {10, 13}}
[]byte{"this,is,another,row"}, [][]int{{0, 4}, {5, 7}, {8, 15}, {16, 19}}
```

It also removes `ReadAll` which is inherently inefficient.
