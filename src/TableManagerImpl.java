import java.util.HashMap;
import java.util.List;
import java.util.ArrayList;
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
import com.apple.foundationdb.KeyValue;
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
              PathUtil.from("database")).join();
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

    // TODO: Create tableName in root. Also create subdirectories for the metadata and rawdata
    try {
      DirectorySubspace table = root.createOrOpen(db, PathUtil.from(tableName)).join();
      DirectorySubspace metadata = table.createOrOpen(db, PathUtil.from("metadata")).join();
      // TODO: Add key-value pairs in the "metadata" table that describes attribute and type (attribute name, type) -> (Pk or No)
      for (int i = 0; i < attributeNames.length; i++) {
        Tuple keyTuple = new Tuple();
        keyTuple = keyTuple.add(attributeNames[i]).add(attributeType[i].name());

        Boolean pk = false;
        for (int j = 0; j < primaryKeyAttributeNames.length; j++) {
          if (attributeNames[i] == primaryKeyAttributeNames[j]) {
            pk = true;
            break;
          }
        }

        Tuple valueTuple = new Tuple();
        valueTuple = valueTuple.add(pk);
        tr.set(metadata.pack(keyTuple), valueTuple.pack());
      }
      table.createOrOpen(db, PathUtil.from("rawdata")).join();
    }
    catch(Exception e) {
      System.out.print("Error adding table");
      tr.commit().join();
      return StatusCode.TABLE_CREATION_ATTRIBUTE_INVALID;
    }

    tr.commit().join();
    return StatusCode.SUCCESS;
  }

  @Override
  public StatusCode deleteTable(String tableName) {
    Transaction tr = db.createTransaction();

    if (!root.exists(db, PathUtil.from(tableName)).join()) return StatusCode.TABLE_NOT_FOUND;

    root.remove(db, PathUtil.from(tableName)).join();
    tr.commit().join();
    return StatusCode.SUCCESS;
  }

  @Override
  public HashMap<String, TableMetadata> listTables() {
    HashMap<String, TableMetadata> tables = new HashMap<>();
    Transaction tr = db.createTransaction();

    // TODO: Iterate through DirectoryLayer and create TableMetadata for each Directory. Add each to HashMap and return.
    List<String> tableNames = root.list(db, PathUtil.from()).join();
    for (String tableName : tableNames) {
      // TODO: make TableMetadata for each tableName
      Range range = root.open(db, PathUtil.from(tableName, "metadata")).join().range();
      List<KeyValue> keyvalues = tr.getRange(range).asList().join();

      ArrayList<String> attributeNames = new ArrayList<>();
      ArrayList<AttributeType> attributeTypes = new ArrayList<>();
      ArrayList<String> primaryKeys = new ArrayList<>();

      for (KeyValue keyvalue : keyvalues) {
        Tuple key = Tuple.fromBytes(keyvalue.getKey());
        attributeNames.add(key.getString(1));
        attributeTypes.add(AttributeType.valueOf(key.getString(2)));
        Tuple value = Tuple.fromBytes(keyvalue.getValue());
        if (value.getBoolean(0) == true) primaryKeys.add(key.getString(1));
      }

      String[] names = new String[attributeNames.size()];
      names = attributeNames.toArray(names);
      AttributeType[] types = new AttributeType[attributeTypes.size()];
      types = attributeTypes.toArray(types);
      String[] pks = new String[primaryKeys.size()];
      pks = primaryKeys.toArray(pks);

      tables.put(tableName, new TableMetadata(names, types, pks));
    }
    tr.commit().join();
    return tables;
  }

  @Override
  public StatusCode addAttribute(String tableName, String attributeName, AttributeType attributeType) {
    Transaction tr = db.createTransaction();

    // TODO: Check if tableName exists in DirectoryLayer.
    if (!root.exists(db, PathUtil.from(tableName)).join()) return StatusCode.TABLE_NOT_FOUND;

    // TODO: Check if attribute exists. If yes, return ATTRIBUTE_ALREADY_EXISTS
    Range range = root.open(db, PathUtil.from(tableName, "metadata")).join().range();
    List<KeyValue> keyvalues = tr.getRange(range).asList().join();
    for (KeyValue keyvalue : keyvalues) {
      Tuple key = Tuple.fromBytes(keyvalue.getKey());
      if (attributeName == key.getString(2)) {
        tr.commit().join();
        return StatusCode.ATTRIBUTE_ALREADY_EXISTS;
      }
    }

    // Add attribute
    DirectorySubspace metadata = root.open(db, PathUtil.from(tableName, "metadata")).join();

    Tuple keyTuple = new Tuple();
    keyTuple = keyTuple.add(attributeName).add(attributeType.name());
    Tuple valueTuple = new Tuple();
    valueTuple = valueTuple.add(false);

    tr.set(metadata.pack(keyTuple), valueTuple.pack());

    tr.commit().join();
    return StatusCode.SUCCESS;
  }

  @Override
  public StatusCode dropAttribute(String tableName, String attributeName) {
    // Check if table exists. If no, return TABLE_NOT_FOUND


    // Check if attribute exists. If no, return ATTRIBUTE_NOT_FOUND

    // TODO: Drop all DB entries with the tableName and attributeName in tuple
    Transaction tr = db.createTransaction();
    try {
      //Open Database

      // Remove all key-value pairs with keys with tableName and attributeName in tuple.
      Tuple tuple = Tuple.from(tableName, attributeName);
      Range range = tuple.range();
      tr.clear(range);

    }
    catch(Exception e) {
      System.out.println("Error");
    }
    finally {
    }

    // Remove attribute from TableMetadata
    tr.commit().join();
    return StatusCode.SUCCESS;
  }

  @Override
  public StatusCode dropAllTables() {
    // Clear all key-value pairs
    Transaction tr = db.createTransaction();
    List<String> tableNames = root.list(db, PathUtil.from()).join();

    for (String tableName : tableNames) {
      // TODO: make TableMetadata for each tableName
      root.remove(db, PathUtil.from(tableName)).join();
    }
    tr.commit().join();

    return StatusCode.SUCCESS;
  }
}
