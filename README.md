# DATABASE DBAL

Just to extend `Doctrine DBAL` in an easier way for `lazy people`

[![Build Status](https://travis-ci.org/pentagonal/DotrineDBALExtender.svg?branch=master)](https://travis-ci.org/pentagonal/DotrineDBALExtender)

## USAGE

**CREATING OBJECT**

```php
<?php
use Pentagonal\DatabaseDBAL\Database;

$config = [
    /**
     * use key with: host, hostname or dbhostname is also accepted
     * can be @uses Database::DB_HOST as key 
     */ 
    'dbhost' => 'localhost',
    /**
     * use key with: user, username, dbuser or dbusername is also accepted
     * can be @uses Database::DB_USER as key 
     */ 
    'dbuser' => 'my_database',
    /**
     * use key with: password, pass or dbpassword is also accepted
     * can be @uses Database::DB_PASSWORD as key 
     */
    'dbpass' => 'my_database',
    /**
     * use key with: name is also accepted,
     * or if you have been determine dbpath or path
     * and your database name is empty, it will
     * use path (@uses Database::DB_PATH) as database name
     * can be @uses Database::DB_NAME as key
     */
    'dbname' => 'my_database',
    /**
     * use key with: driver is also accepted
     * can be @uses Database::DB_NAME as key  
     */ 
    'dbdriver' => 'mysql',
    /**
     * use key with: prefix is also accepted
     * can be @uses Database::DB_PREFIX as key
     * this prefix is for manually handle prefix with your
     * query 
     */ 
    'dbprefix' => 'prefix_',
];

/**
 * @var Database $db 
 */
$db = Database::create($config);

```
NOTE:


```txt
The Database is use magic method of class :

    Doctrine\DBAL\Connection

so if you hav call the method that does not exists on Database object
it will be call the magic method to
    
    Doctrine\DBAL\Connection::method(...$params) 

```

## EASY USAGE FOR LAZY

```php
<?php
use Pentagonal\DatabaseDBAL\Database;

$db = Database::create([
    // Database Name, User is Important
    // Password maybe can be use as empty value if you have no password
    Database::DB_NAME => 'my_database',
    Database::DB_USER => 'username',
    Database::DB_PASSWORD => 'password',
    // Database host maybe empty on mysql host
    // and will be append as localhost / 127.0.0.1
    Database::DB_HOST => 'localhost',
    // by default if you use port 3306 it will be automatically use mysql
    Database::DB_PORT => 3306,
    // or if you have been empty the port (or set null)
    // and driver has not selected it will be use mysql as driver
    Database::DB_DRIVER => 'pdo_mysql',
    Database::DB_PREFIX => 'prefix_',
]);
/**
 * @var string[]|string $tableName
 * You can use array|object|string 
 */
$tableName = 'my_table';
/**
 * @var bool $useIdentifier
 * to prefix with append identifier eg mysql is back tick (`) 
 */
$useIdentifier = true;
// quote table prefix
$tablePrefixed = $db->prefix($tableName, $useIdentifier);
// prefixTables & prefix is alias
$tablePrefixed = $db->prefixTables($tableName, $useIdentifier);

/**
 * Execute Lazy Query with question mark placeholder 
 * @var Doctrine\DBAL\Driver\Statement $stmtQuery
 */
$stmtQuery = $db->queryBind(
    "SELECT * FROM {$tablePrefixed} WHERE first_column=? AND second_column=?",
    [
        'first column value',
        'second column value',
    ]
);

// get result
$result = [];
/**
 * @uses \PDO constant as fetch mode  
 */
while ($row = $stmtQuery->fetch(\PDO::FETCH_ASSOC)) {
    $result[] = $row;
}
$stmtQuery->closeCursor();

$stmtPrepareExecute = $db->executePrepare(
    "SELECT * FROM {$tablePrefixed}
        WHERE first_column=:first
            AND second_column=:second
    ",
    [
        ':first' => 'first column value',         
        ':second' => 'second column value',     
    ]
);

$stmtPrepareExecute->fetchAll();
$stmtPrepareExecute->closeCursor();

```

## REQUIREMENTS

```
- php 7.0 or later
- Pdo Php extension

Suggest:

- ext-iconv 
- ext-mb_string

```

## INSTALLING

```json
{
  "require": {
    "pentagonal/database-dbal": "~1"
  }
}
```

## LICENSE

`MIT License` @see [LICENSE](LICENSE)
