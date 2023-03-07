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

  private FDB fdbAPI;
  private Database db;
  private DirectorySubspace root;

  public TableManagerImpl() {
    // Instantiate the Database and open it
    fdbAPI = FDB.selectAPIVersion(710);
    try {
      db = fdbAPI.open();
    }
    catch(Exception e) {
      System.out.println("Failed to open database");
    }

    // Instantiate the root directory
    try {
      root = DirectoryLayer.getDefault().createOrOpen(db,
              PathUtil.from("Database")).join();
    }
    catch(Exception e) {
      System.out.println("Failed to create root directory");
    }
  }

  @Override
  public StatusCode createTable(String tableName, String[] attributeNames, AttributeType[] attributeType,
                         String[] primaryKeyAttributeNames) {
    Transaction tr = db.createTransaction();

    // TODO: Check if table name already exists. Look for tableName in DirectoryLayer
    if (root.exists(db, PathUtil.from(tableName)).join()) return StatusCode.TABLE_ALREADY_EXISTS;

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

    // TODO: Create a Directory with name tableName at root

    return StatusCode.SUCCESS;
  }

  @Override
  public StatusCode deleteTable(String tableName) {
    // TODO: Check if tableName exists in DirectoryLayer.

    // TOOD: Remove the Directory
    try {
      // Create transaction
      Transaction tr = db.createTransaction();

      // TODO: Remove all key-value pairs in Directory and remove the Directory
      Tuple tuple = Tuple.from(tableName);
      Range range = tuple.range();
      tr.clear(range);

      // Commit the transaction
      tr.commit().join();
    }
    catch(Exception e) {
      System.out.println("Error");
    }

    return StatusCode.SUCCESS;
  }

  @Override
  public HashMap<String, TableMetadata> listTables() {
    // TODO: Iterate through DirectoryLayer and create TableMetadata for each Directory. Add each to HashMap and return.

    // Dummy hash map
    HashMap<String, TableMetadata> tables = new HashMap<>();
    return tables;
  }

  @Override
  public StatusCode addAttribute(String tableName, String attributeName, AttributeType attributeType) {

    // TODO: Check if tableName exists in DirectoryLayer.

    // TODO: Check if attribute exists. If yes, return ATTRIBUTE_ALREADY_EXISTS

    // TODO:

    return StatusCode.SUCCESS;
  }

  @Override
  public StatusCode dropAttribute(String tableName, String attributeName) {
    // Check if table exists. If no, return TABLE_NOT_FOUND


    // Check if attribute exists. If no, return ATTRIBUTE_NOT_FOUND

    // TODO: Drop all DB entries with the tableName and attributeName in tuple
    try {
      //Open Database
      Transaction tr = db.createTransaction();

      // Remove all key-value pairs with keys with tableName and attributeName in tuple.
      Tuple tuple = Tuple.from(tableName, attributeName);
      Range range = tuple.range();
      tr.clear(range);

      // TODO: Commit the transaction
      //tr.commit().join();
    }
    catch(Exception e) {
      System.out.println("Error");
    }

    // Remove attribute from TableMetadata

    return StatusCode.SUCCESS;
  }

  @Override
  public StatusCode dropAllTables() {
    // Clear all key-value pairs


    // Clear hash map

    return StatusCode.SUCCESS;
  }
}
