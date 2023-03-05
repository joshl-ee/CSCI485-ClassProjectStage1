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

    // Remove table-specific key-value pairs
    try {
      //Open Database
      Database db = fdbAPI.open();
      Transaction tr = db.createTransaction();

      // Remove all key-value pairs with keys with tableName in tuple. TODO: **Check if implemented correctly**
      byte[] prefix = tableName.getBytes();
      byte[] end = KeySelector.firstGreaterThan(prefix).getKey();
      Range range = new Range(prefix, end);
      tr.clear(range);

      // TODO: Commit the transaction
      //tr.commit().join();
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
    // Employee(Name, SSID)
    // bob,   1
    // josh, 2
    // addAttribute( employee, favoriteColor)

    //   key                value
    // Employee, 1 ,Name -> bob
    // Employee, 1, SSID -> 1
    // Employee, 1, favoriteColor -> color
    // Employee, 2, Name -> josh
    // Employee, 2, SSID -> 2

    // Check if table exists. If no, return TABLE_NOT_FOUND
    if (!tables.containsKey(tableName)) {
      return StatusCode.TABLE_NOT_FOUND;
    }
    // Check if attribute exists. If yes, return ATTRIBUTE_ALREADY_EXISTS
    TableMetadata table = tables.get(tableName);
    if (table.doesAttributeExist(attributeName)) {
      return StatusCode.ATTRIBUTE_ALREADY_EXISTS;
    }

    // Update TableMetadata for given table
    table.addAttribute(attributeName, attributeType);

    return StatusCode.SUCCESS;
  }

  @Override
  public StatusCode dropAttribute(String tableName, String attributeName) {
    // Check if table exists. If no, return TABLE_NOT_FOUND
    if (!tables.containsKey(tableName)) {
      return StatusCode.TABLE_NOT_FOUND;
    }

    // Check if attribute exists. If no, return ATTRIBUTE_NOT_FOUND
    TableMetadata table = tables.get(tableName);
    if (!table.doesAttributeExist(attributeName)) {
      return StatusCode.ATTRIBUTE_NOT_FOUND;
    }

    // TODO: Drop all DB entries with the tableName and attributeName in tuple


    // Remove attribute from TableMetadata
    table.getAttributes().remove(attributeName);

    return StatusCode.SUCCESS;
  }

  @Override
  public StatusCode dropAllTables() {
    // Clear all key-value pairs
    try {
      //Open Database
      Database db = fdbAPI.open();
      Transaction tr = db.createTransaction();

      // TODO: Remove all key-value pairs. **Check if implemented correctly**
      byte[] prefix = new byte[]{};
      byte[] end = new byte[]{(byte) 0xff};
      Range range = new Range(prefix, end);
      tr.clear(range);

      // Commit the transaction
      //tr.commit().join();
    }
    catch(Exception e) {
      System.out.println("Error");
    }

    // Clear hash map
    tables.clear();

    return StatusCode.SUCCESS;
  }
}
