package memstore.table;

import it.unimi.dsi.fastutil.ints.IntArrayList;
import memstore.data.ByteFormat;
import memstore.data.DataLoader;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * IndexedRowTable, which stores data in row-major format.
 * That is, data is laid out like
 *   row 1 | row 2 | ... | row n.
 *
 * Also has a tree index on column `indexColumn`, which points
 * to all row indices with the given value.
 */
public class IndexedRowTable implements Table {

    int numCols;
    int numRows;
    private TreeMap<Integer, IntArrayList> index;
    private ByteBuffer rows;
    private int indexColumn;

    public IndexedRowTable(int indexColumn) {
        this.indexColumn = indexColumn;
    }

    /**
     * Loads data into the table through passed-in data loader. Is not timed.
     *
     * @param loader Loader to load data from.
     * @throws IOException
     */
    @Override
    public void load(DataLoader loader) throws IOException {
        this.numCols = loader.getNumCols();
        List<ByteBuffer> rows = loader.getRows();
        numRows = rows.size();
        this.index = new TreeMap<>();
        this.rows = ByteBuffer.allocate(ByteFormat.FIELD_LEN * numRows * numCols);

        for (int rowId = 0; rowId < numRows; rowId++) {
            ByteBuffer curRow = rows.get(rowId);
            for (int colId = 0; colId < numCols; colId++) {
                int offset = ByteFormat.FIELD_LEN * ((rowId * numCols) + colId);
                this.rows.putInt(offset, curRow.getInt(ByteFormat.FIELD_LEN * colId));
            }
            // update index tree
            int key = getIntField(rowId, indexColumn);
            IntArrayList value = this.index.getOrDefault(key, new IntArrayList());
            index.put(key, value);
            value.add(rowId);
        }
    }

    /**
     * Returns the int field at row `rowId` and column `colId`.
     */
    @Override
    public int getIntField(int rowId, int colId) {
        return rows.getInt(ByteFormat.FIELD_LEN * (rowId * numCols + colId));
    }

    /**
     * Inserts the passed-in int field at row `rowId` and column `colId`.
     */
    @Override
    public void putIntField(int rowId, int colId, int field) {
        int offset = ByteFormat.FIELD_LEN * ((rowId * numCols) + colId);
        this.rows.putInt(offset, field);
    }

    /**
     * Implements the query
     *  SELECT SUM(col0) FROM table;
     *
     *  Returns the sum of all elements in the first column of the table.
     */
    @Override
    public long columnSum() {
        long result = 0;
        for (int rowId = 0; rowId < numRows; rowId++) {
            result += getIntField(rowId, 0);
        }
        return result;
    }

    /**
     * Implements the query
     *  SELECT SUM(col0) FROM table WHERE col1 > threshold1 AND col2 < threshold2;
     *
     *  Returns the sum of all elements in the first column of the table,
     *  subject to the passed-in predicates.
     */
    @Override
    public long predicatedColumnSum(int threshold1, int threshold2) {
        long result = 0;
        if (indexColumn == 1) {
            while (true) {
                Map.Entry<Integer, IntArrayList> higherEntry = index.higherEntry(threshold1);
                if (higherEntry == null) {
                    break;
                }
                int threshold = threshold2;
                result += higherEntry
                        .getValue()
                        .stream()
                        .filter(rowId -> getIntField(rowId, 2) < threshold)
                        .mapToInt(rowId -> getIntField(rowId, 0))
                        .sum();
                threshold1 = higherEntry.getKey();
            }
        } else if (indexColumn == 2) {
            while (true) {
                Map.Entry<Integer, IntArrayList> lowerEntry = index.lowerEntry(threshold2);
                if (lowerEntry == null) {
                    break;
                }
                int threshold = threshold1;
                result += lowerEntry
                        .getValue()
                        .stream()
                        .filter(rowId -> getIntField(rowId, 1) > threshold)
                        .mapToInt(rowId -> getIntField(rowId, 0))
                        .sum();
                threshold2 = lowerEntry.getKey();
            }
        } else {
            for (int rowId = 0; rowId < numRows; rowId++) {
                if (getIntField(rowId, 1) > threshold1 && getIntField(rowId, 2) < threshold2) {
                    result += getIntField(rowId, 0);
                }
            }
        }
        return result;
    }

    /**
     * Implements the query
     *  SELECT SUM(col0) + SUM(col1) + ... + SUM(coln) FROM table WHERE col0 > threshold;
     *
     *  Returns the sum of all elements in the rows which pass the predicate.
     */
    @Override
    public long predicatedAllColumnsSum(int threshold) {
        long result = 0;
        if (indexColumn == 0) {
            while (true) {
                Map.Entry<Integer, IntArrayList> higherEntry = index.higherEntry(threshold);
                if (higherEntry == null) {
                    break;
                }
                result += higherEntry
                        .getValue()
                        .stream()
                        .mapToInt(rowId -> {
                            int sum = 0;
                            for (int colId = 0; colId < numCols; colId++) {
                                sum += getIntField(rowId, colId);
                            }
                            return sum;
                        })
                        .sum();
                threshold = higherEntry.getKey();
            }
        } else {
            for (int rowId = 0; rowId < numRows; rowId++) {
                if (getIntField(rowId, 0) <= threshold) {
                    continue;
                }
                for (int colId = 0; colId < numCols; colId++) {
                    result += getIntField(rowId, colId);
                }
            }
        }
        return result;
    }

    /**
     * Implements the query
     *   UPDATE(col3 = col3 + col2) WHERE col0 < threshold;
     *
     *   Returns the number of rows updated.
     */
    @Override
    public int predicatedUpdate(int threshold) {
        int affected = 0;
        if (0 == indexColumn) {
            while (true) {
                Map.Entry<Integer, IntArrayList> lowerEntry = index.lowerEntry(threshold);
                if (lowerEntry == null) {
                    break;
                }
                affected += lowerEntry.getValue().size();
                lowerEntry.getValue()
                        .stream()
                        .forEach(rowId -> putIntField(rowId, 3, getIntField(rowId, 3) + getIntField(rowId, 2)));
                threshold = lowerEntry.getKey();
            }
        } else {
            for (int rowId = 0; rowId < numRows; rowId++) {
                if (getIntField(rowId, 0) >= threshold) {
                    continue;
                }
                affected++;
                putIntField(rowId, 3, getIntField(rowId, 3) + getIntField(rowId, 2));
            }
        }
        return affected;
    }
}
