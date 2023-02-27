import java.util.HashMap;
import java.lang.String;

import com.apple.foundationdb.Database;

import com.apple.foundationdb.FDB;
import com.apple.foundationdb.KeySelector;
import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.Range;
import com.apple.foundationdb.directory.DirectoryLayer;
import com.apple.foundationdb.directory.DirectorySubspace;
import com.apple.foundationdb.directory.PathUtil;
import com.apple.foundationdb.tuple.Tuple;
/**
 * TableManagerImpl implements interfaces in {#TableManager}. You should put your implementation
 * in this class.
 */
public class TableManagerImpl implements TableManager{

  HashMap<String, TableMetadata> tables;
  FDB fdbAPI;

  public TableManagerImpl() {
    tables = new HashMap<>();
    fdbAPI = FDB.selectAPIVersion(710);
  }

  @Override
  public StatusCode createTable(String tableName, String[] attributeNames, AttributeType[] attributeType,
                         String[] primaryKeyAttributeNames) {

    // Check if table name is unique
    for (String table : tables.keySet()) {
      if (tableName.equals(table)) return StatusCode.TABLE_ALREADY_EXISTS;
    }

    // Check if attribute parameters are provided and valid
    if (attributeNames == null || attributeType == null ||
            primaryKeyAttributeNames == null || attributeNames.length != attributeType.length) return StatusCode.TABLE_CREATION_ATTRIBUTE_INVALID;

    // Primary key attributes contains attributes that are not in the attribute definitions
    for (String pk : primaryKeyAttributeNames) {
      boolean contains = false;
      for (String attribute : attributeNames) {
        if (pk.equals(attribute)) {
          contains = true;
          break;
        }
      }
      if (!contains) return StatusCode.TABLE_CREATION_PRIMARY_KEY_NOT_FOUND;
    }

    // No errors, add it
    tables.put(tableName, new TableMetadata(attributeNames, attributeType, primaryKeyAttributeNames));
    return StatusCode.SUCCESS;
  }

  @Override
  public StatusCode deleteTable(String tableName) {
    // Table doesn't exist
    if (tables.get(tableName) == null) return StatusCode.TABLE_NOT_FOUND;

    // Remove rows from FoundationDB
    try {
      Database db = fdbAPI.open();
      Transaction tr = db.createTransaction();
      // Remove all key-value pairs with keys starting with "prefix"
      byte[] prefix = tableName.getBytes();
      byte[] end = KeySelector.firstGreaterThan(prefix).getKey();
      Range range = new Range(prefix, end);
      tr.clear(range);

      // Commit the transaction
      tr.commit().join();
    }
    catch(Exception e) {
      System.out.println("Error");
    }

    // Remove table from our records
    tables.remove(tableName);

    return StatusCode.SUCCESS;
  }

  @Override
  public HashMap<String, TableMetadata> listTables() {
    return tables;
  }

  @Override
  public StatusCode addAttribute(String tableName, String attributeName, AttributeType attributeType) {
    // your code
    return StatusCode.SUCCESS;
  }

  @Override
  public StatusCode dropAttribute(String tableName, String attributeName) {
    // your code
    return StatusCode.SUCCESS;
  }

  @Override
  public StatusCode dropAllTables() {
    // your code

    // Clear all entries in db

    // Clear hash map
    tables.clear();
    return StatusCode.SUCCESS;
  }
}
