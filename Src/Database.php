<?php
/**
 * MIT License
 *
 * Copyright (c) 2017, Pentagonal
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NON INFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */

namespace Pentagonal\DatabaseDBAL;

use Doctrine\Common\EventManager;
use Doctrine\DBAL\Cache\QueryCacheProfile;
use Doctrine\DBAL\Configuration;
use Doctrine\DBAL\Driver;
use Doctrine\DBAL\Connection;
use Doctrine\DBAL\DBALException;
use Doctrine\DBAL\DriverManager;
use Doctrine\DBAL\Platforms\AbstractPlatform;
use Doctrine\DBAL\Query\Expression\ExpressionBuilder;
use Doctrine\DBAL\Query\QueryBuilder;
use Doctrine\DBAL\Schema\AbstractSchemaManager;
use Doctrine\DBAL\Driver\Statement;
use Doctrine\DBAL\Driver\ResultStatement;
use Doctrine\DBAL\Schema\Column;
use Doctrine\DBAL\Schema\ForeignKeyConstraint;
use Doctrine\DBAL\Schema\Index;
use Doctrine\DBAL\Schema\Sequence;
use Doctrine\DBAL\Schema\Table;
use Doctrine\DBAL\Schema\View;

/**
 * Class Database
 * @package Pentagonal\PenTaleGram\Core
 *
 * Magic Method @see Database::__call()
 *      -> @uses Connection
 *
 * @method void beginTransaction()
 *
 * @method void         close()
 * @method void         commit()
 * @method bool         connect()
 * @method mixed        convertToDatabaseValue(mixed $value, $type)
 * @method mixed        convertToPHPValue(mixed $value, $type)
 * @method QueryBuilder createQueryBuilder()
 * @method void         createSavepoint(string $savepoint)
 *
 * @method int delete(string $tableExpression, array $identifier, array $types = [])
 *
 * @method int       errorCode()
 * @method array     errorInfo()
 * @method void      exec(string $statement)
 * @method Statement executeQuery(string $query, array $params = [], array $types = [], QueryCacheProfile $qcp = null)
 * @method ResultStatement executeCacheQuery(string $query, $params, $types, QueryCacheProfile $qcp)
 * @method int       executeUpdate(string $query, array $params = [], array $types = [])
 *
 * @method int  insert(string $tableExpression, array $data, array $types = [])
 * @method bool isAutoCommit()
 * @method bool isConnected()
 * @method bool isRollbackOnly()
 * @method bool isTransactionActive()
 *
 * @method array fetchAssoc(string $statement, array $params = [], array $types = [])
 * @method array fetchArray(string $statement, array $params = [], array $types = [])
 * @method array fetchColumn(string $statement, array $params = [], array $types = [])
 * @method array fetchAll(string $sql, array $params = array(), $types = array())
 *
 * @method Configuration         getConfiguration()
 * @method Driver                getDriver()
 * @method string                getDatabase()
 * @method AbstractPlatform      getDatabasePlatform()
 * @method EventManager          getEventManager()
 * @method ExpressionBuilder     getExpressionBuilder()
 * @method string                getHost()
 * @method array                 getParams()
 * @method string|null           getPassword()
 * @method mixed                 getPort()
 * @method AbstractSchemaManager getSchemaManager()
 * @method int                   getTransactionIsolation()
 * @method int                   getTransactionNestingLevel()
 * @method string|null           getUsername()
 * @method Connection            getWrappedConnection()
 *
 * @method string lastInsertId(string|null $seqName)
 *
 * @method bool      ping()
 * @method Statement prepare(string $statement)
 * @method array     project(string $query, array $params, \Closure $function)
 *
 * @method void      releaseSavepoint(string $savePoint)
 * @method array     resolveParams(array $params, array $types)
 * @method bool|void rollBack()
 * @method void      rollbackSavepoint(string $savePoint)
 *
 * @method void setAutoCommit(bool $autoCommit)
 * @method void setFetchMode(int $fetchMode)
 * @method void setNestTransactionsWithSavePoints(bool $nestTransactionsWithSavePoints)
 * @method void setRollbackOnly()
 * @method int  setTransactionIsolation(int $level)
 *
 * @method void transactional(\Closure $func)
 *
 * @method int update(string $tableExpression, array $data, array $identifier, array $types = [])
 *
 * @method string    quote(mixed $input, int $type = \PDO::PARAM_STR)
 * @method string    quoteIdentifier(string $str)
 *
 * @uses \PDO::ATTR_DEFAULT_FETCH_MODE for (19)
 * @method Statement query(string $sql, int $mode = 19, mixed $additionalArg = null, array $constructorArgs = [])
 */
class Database
{
    /**
     * @var StringSanitizerTrait
     * use for sanitize database
     */
    use StringSanitizerTrait;

    /**
     * @see Connection::TRANSACTION_READ_UNCOMMITTED
     */
    const TRANSACTION_READ_UNCOMMITTED = Connection::TRANSACTION_READ_UNCOMMITTED;

    /**
     * @see Connection::TRANSACTION_READ_COMMITTED
     */
    const TRANSACTION_READ_COMMITTED   = Connection::TRANSACTION_READ_COMMITTED;

    /**
     * @see Connection::TRANSACTION_REPEATABLE_READ
     */
    const TRANSACTION_REPEATABLE_READ = Connection::TRANSACTION_REPEATABLE_READ;

    /**
     * @see Connection::TRANSACTION_SERIALIZABLE
     */
    const TRANSACTION_SERIALIZABLE = Connection::TRANSACTION_SERIALIZABLE;

    /**
     * @see Connection::PARAM_INT_ARRAY
     */
    const PARAM_INT_ARRAY = Connection::PARAM_INT_ARRAY;

    /**
     * @see Connection::PARAM_STR_ARRAY
     */
    const PARAM_STR_ARRAY = Connection::PARAM_STR_ARRAY;

    /**
     * @see Connection::ARRAY_PARAM_OFFSET
     */
    const ARRAY_PARAM_OFFSET = Connection::ARRAY_PARAM_OFFSET;

    /**
     * @var Connection
     */
    protected $currentConnection;

    /**
     * @var string
     */
    protected $currentSelectedDriver;

    /**
     * @var array
     */
    protected $currentUserParams = [];

    /**
     * @var array
     */
    protected $defaultParams = [
        self::DB_CHARSET => self::DEFAULT_CHARSET,
    ];

    /**
     * @var string
     */
    protected $currentTablePrefix = '';

    /**
     * Column Quote Identifier
     *
     * @var string
     */
    protected $currentQuoteIdentifier = '"';

    /**
     * Default Driver
     *
     * @var string
     */
    protected $defaultDriver = 'pdo_mysql';

    /**
     * Default character Set
     */
    const DEFAULT_CHARSET = 'UTF8';

    /**
     * Setting
     */
    const
        DB_HOST = 'host',
        DB_USER = 'user',
        DB_NAME = 'dbname',
        DB_PASSWORD = 'password',
        DB_DRIVER   = 'driver',
        DB_PATH     = 'path',
        DB_PORT     = 'port',
        DB_PREFIX   = 'prefix',
        DB_PROTOCOL = 'protocol',
        DB_CHARSET  = 'charset',
        DB_TIMEOUT  = 'timeout',
        DB_OPTIONS  = 'options',
        DB_COLLATE  = 'collate';

    /**
     * Driver
     */
    const
        DRIVER_MYSQL   = 'pdo_mysql',
        DRIVER_PGSQL   = 'pdo_pgsql',
        DRIVER_SQLITE  = 'pdo_sqlite',
        DRIVER_DRIZZLE = 'drizzle_pdo_mysql',
        DRIVER_DB2     = 'ibm_db2',
        DRIVER_SQLSRV  = 'pdo_sqlsrv',
        DRIVER_OCI8    = 'oci8';

    /**
     * @var int default timeout
     */
    const DEFAULT_TIMEOUT = 5;

    /**
     * Last configurations param
     *
     * @var array
     */
    protected static $lastParams;

    /**
     * Database constructor.
     * @param array $configs database Configuration
     * @throws DBALException
     */
    public function __construct(array $configs)
    {
        /**
         * Merge User Param
         */
        $this->currentUserParams = $this->normalizeDatabaseParams($configs);

        if (isset($this->currentUserParams[static::DB_PREFIX])) {
            $prefix = $this->currentUserParams[static::DB_PREFIX];
            if (!is_string($prefix) && ! is_bool($prefix) && !is_null($prefix)) {
                throw new \InvalidArgumentException(
                    'Prefix must be as a string %s given.',
                    gettype($prefix)
                );
            }
            if (is_string($prefix)) {
                $this->currentTablePrefix = trim($prefix);
            }
        }

        unset($this->currentUserParams[static::DB_PREFIX]);

        /**
         * Re-Sanitize Selected Driver
         */
        $this->currentSelectedDriver = is_string($this->currentUserParams[self::DB_DRIVER])
            ? $this->normalizeDatabaseDriver($this->currentUserParams[self::DB_DRIVER])
            : null;
        if (!$this->currentSelectedDriver) {
            throw new DBALException('Selected driver unavailable.', E_USER_ERROR);
        }
        if (empty($this->currentUserParams[self::DB_NAME])) {
            throw new DBALException('Database Name could not be empty.', E_USER_ERROR);
        }

        $this->currentUserParams[self::DB_DRIVER] = $this->currentSelectedDriver;
        /**
         * Remove Unwanted Params
         */
        $params = $this->currentUserParams;
        unset($params['pass']);
        foreach ($params as $key => $values) {
            if (is_numeric($key) || $key !== self::DB_NAME && stripos($key, 'db') === 0) {
                unset($params[$key]);
            }
        }
        unset($params[self::DB_OPTIONS]);
        /**
         * Create New Connection
         */
        $this->currentConnection = DriverManager::getConnection($params);
        // set last params
        static::$lastParams = $this->currentUserParams;
        /**
         * Set Quote Identifier
         */
        $this->currentQuoteIdentifier = $this
            ->currentConnection
            ->getDatabasePlatform()
            ->getIdentifierQuoteCharacter();
    }

    /**
     * @param array|null $config
     *
     * @return Database
     * @throws DBALException
     */
    public static function create(array $config = null) : Database
    {
        return !is_array($config)
            // fallback to last param
            ? static::createFromLastParams()
            : new static($config);
    }

    /**
     * @return Database
     * @throws DBALException
     */
    public static function createFromLastParams() : Database
    {
        if (empty(static::$lastParams)) {
            throw new \RuntimeException(
                'Database not being init before',
                E_WARNING
            );
        }

        return new static(static::$lastParams);
    }

    /**
     * Normalize Configurations Param
     *
     * @param array $configs
     *
     * @return array
     */
    final public function normalizeDatabaseParams(array $configs) : array
    {
        if (empty($configs)) {
            return $configs;
        }

        /**
         * Merge User Param
         */
        $currentUserParams = array_merge($this->defaultParams, $configs);

        $toSanity = [
            self::DB_HOST => 'dbhost',
            self::DB_USER => 'dbuser',
            self::DB_NAME => 'name',
            self::DB_PASSWORD => 'dbpass',
            self::DB_DRIVER   => 'dbdriver',
            self::DB_PATH     => 'dbpath',
            self::DB_PORT     => 'dbport',
            self::DB_PREFIX   => 'dbprefix',
            self::DB_PROTOCOL => 'dbprotocol',
            self::DB_CHARSET  => 'dbcharset',
            self::DB_COLLATE  => 'dbcollate',
            self::DB_TIMEOUT  => 'dbtimeout',
            self::DB_OPTIONS  => 'dboptions',
        ];

        foreach ($toSanity as $key => $name) {
            if ($key === $name) {
                continue;
            }
            if (!isset($currentUserParams[$key]) && isset($currentUserParams[$name])) {
                $currentUserParams[$key] = $currentUserParams[$name];
            }
        }

        $toSanity = [
            self::DB_HOST => 'db_host',
            self::DB_USER => 'db_user',
            self::DB_NAME => 'db_name',
            self::DB_PASSWORD => 'db_pass',
            self::DB_DRIVER   => 'db_driver',
            self::DB_PATH     => 'db_path',
            self::DB_PORT     => 'db_port',
            self::DB_PREFIX   => 'db_prefix',
            self::DB_PROTOCOL => 'db_protocol',
            self::DB_CHARSET  => 'db_charset',
            self::DB_COLLATE  => 'db_collate',
            self::DB_TIMEOUT  => 'db_timeout',
            self::DB_OPTIONS  => 'db_options',
        ];
        foreach ($toSanity as $key => $name) {
            if ($key === $name) {
                continue;
            }
            if (!isset($currentUserParams[$key]) && isset($currentUserParams[$name])) {
                $currentUserParams[$key] = $currentUserParams[$name];
            }
        }

        // re-sanitize db host
        if (!isset($currentUserParams[self::DB_HOST])) {
            if (isset($currentUserParams['hostname'])) {
                $currentUserParams[self::DB_HOST] = $currentUserParams['hostname'];
            } elseif (isset($currentUserParams['dbhostname'])) {
                $currentUserParams[self::DB_HOST] = $currentUserParams['dbhostname'];
            }
        }

        // re-sanitize db password
        if (!isset($currentUserParams[self::DB_PASSWORD])) {
            if (isset($currentUserParams['dbpassword'])) {
                $currentUserParams[self::DB_PASSWORD] = $currentUserParams['dbpassword'];
            } elseif (isset($currentUserParams['pass'])) {
                $currentUserParams[self::DB_PASSWORD] = $currentUserParams['pass'];
            }
        }

        // re-sanitize db user
        if (!isset($currentUserParams[self::DB_USER])) {
            if (isset($currentUserParams['dbusername'])) {
                $currentUserParams[self::DB_USER] = $currentUserParams['dbusername'];
            } elseif (isset($currentUserParams['username'])) {
                $currentUserParams[self::DB_USER] = $currentUserParams['username'];
            }
        }


        /**
         * check if port in 3306 & empty driver
         */
        if (empty($currentUserParams[self::DB_DRIVER])
            && isset($currentUserParams[self::DB_PORT])
            && abs($currentUserParams[self::DB_PORT]) === 3306
        ) {
            $currentUserParams[self::DB_DRIVER] = 'pdo_mysql';
        }

        if (empty($currentUserParams[self::DB_DRIVER])) {
            $currentUserParams[self::DB_DRIVER] = $this->defaultDriver;
        }

        if (!empty($currentUserParams[self::DB_DRIVER])) {
            $currentUserParams[self::DB_DRIVER] = $this
                ->normalizeDatabaseDriver($currentUserParams[self::DB_DRIVER]);
        }

        if (empty($currentUserParams[self::DB_PORT])
            && $currentUserParams[self::DB_DRIVER] === 'pdo_mysql'
        ) {
            $currentUserParams[self::DB_PORT] = 3306;
        }

        if (!empty($currentUserParams[self::DB_DRIVER])
            && $currentUserParams[self::DB_DRIVER] == 'pdo_sqlite'
        ) {
            if (empty($currentUserParams[self::DB_PATH])) {
                if (isset($currentUserParams[self::DB_NAME])
                    && is_string($currentUserParams[self::DB_NAME])
                ) {
                    $currentUserParams[self::DB_PATH] = $currentUserParams[self::DB_NAME];
                }
            } elseif (empty($currentUserParams[self::DB_NAME])) {
                if (isset($currentUserParams[self::DB_PATH])
                    && is_string($currentUserParams[self::DB_PATH])
                ) {
                    $currentUserParams[self::DB_NAME] = $currentUserParams[self::DB_PATH];
                }
            }
        }

        $charset = self::DEFAULT_CHARSET;
        if (is_string($currentUserParams[self::DB_CHARSET])
            && strpos($currentUserParams[self::DB_CHARSET], '-') !== false
        ) {
            $currentUserParams[self::DB_CHARSET] = str_replace(
                '-',
                '',
                trim(strtoupper($currentUserParams[self::DB_CHARSET]))
            );
        }

        if (isset($currentUserParams[self::DB_COLLATE])) {
            $collate = isset($currentUserParams[self::DB_COLLATE])
                ? $currentUserParams[self::DB_COLLATE]
                : null;
            if (!is_string($collate) && $currentUserParams[self::DB_DRIVER] === 'mysql') {
                $collate = 'utf8_unicode_ci';
            }
            $collate = preg_replace('`(\-|\_)+`', '_', $collate);
            $collate = trim(strtolower($collate));
            $collateArray = explode('_', $collate);
            $charset = reset($collateArray)?: $charset;
        }

        if (!is_string($currentUserParams[self::DB_CHARSET])
            || trim($currentUserParams[self::DB_CHARSET]) === ''
        ) {
            $currentUserParams[self::DB_CHARSET] = $charset;
        }

        $defaultDriverOptions = [
            \PDO::ATTR_TIMEOUT => static::DEFAULT_TIMEOUT,
            \PDO::ATTR_ERRMODE => \PDO::ERRMODE_EXCEPTION
        ];

        $defaultOptions = false;
        if (!isset($currentUserParams[self::DB_OPTIONS])
            || !is_array($currentUserParams[self::DB_OPTIONS])
        ) {
            $defaultOptions    = true;
            $currentUserParams[self::DB_OPTIONS] = $defaultDriverOptions;
        }

        if (!isset($currentUserParams['driverOptions'])
            || !is_array($currentUserParams['driverOptions'])
        ) {
            $currentUserParams['driverOptions'] = null;
        }

        if (!is_array($currentUserParams['driverOptions'])) {
            $currentUserParams['driverOptions'] = [];
        }

        foreach ($currentUserParams['driverOptions'] as $key => $val) {
            if (!is_int($key) || !is_int($val)) {
                if ($key === \PDO::ATTR_TIMEOUT && is_float($val)) {
                    continue;
                }
                unset($currentUserParams['driverOptions'][$key]);
                continue;
            }
        }
        foreach ($currentUserParams[self::DB_OPTIONS] as $key => $val) {
            if (!is_int($key) || !is_int($val) || isset($currentUserParams['driverOptions'][$key])) {
                continue;
            }
            $currentUserParams['driverOptions'][$key] = $val;
        }

        // default fallback to exception
        $currentUserParams['driverOptions'][\PDO::ATTR_ERRMODE] = \PDO::ERRMODE_EXCEPTION;

        if (isset($currentUserParams[self::DB_TIMEOUT])) {
            // timeout
            $timeout = $currentUserParams[self::DB_TIMEOUT];
            $timeout = ! is_numeric($timeout) ? static::DEFAULT_TIMEOUT : intval($timeout);
            $timeout = !is_int($timeout) ? 10 : $timeout;
            $timeout = $timeout < 1 ? 5 : $timeout;
            $currentUserParams[self::DB_TIMEOUT] = $timeout;
        }
        if (isset($timeout) || (
                !isset($currentUserParams['driverOptions'][\PDO::ATTR_TIMEOUT])
                || !is_numeric($currentUserParams['driverOptions'][\PDO::ATTR_TIMEOUT])
            )
        ) {
            $currentUserParams['driverOptions'][\PDO::ATTR_TIMEOUT] = $timeout;
        }

        // correcting host
        if (!isset($currentUserParams[self::DB_HOST])
            || !$currentUserParams[self::DB_HOST]
        ) {
            $currentUserParams[self::DB_HOST] = 'localhost';
        }

        $currentUserParams['options'] = $currentUserParams['driverOptions'];
        return $currentUserParams;
    }

    /**
     * Aliases
     *
     * please @uses normalizeDatabaseDriver()
     *
     * @param string $driver
     *
     * @return bool|string
     */
    final public function sanitizeSelectedAvailableDriver(string $driver)
    {
        return $this->normalizeDatabaseDriver($driver);
    }

    /**
     * Check Database driver available for Doctrine
     * and choose the best driver of sqlsrv an oci
     *
     * @param string $driverName
     * @final
     * @return bool|string return lowercase an fix database driver for Connection
     */
    final public function normalizeDatabaseDriver(string $driverName)
    {
        if (is_string($driverName) && trim($driverName)) {
            $driverName = trim(strtolower($driverName));
            // maria-db is a mysql, there was much of unknown people use it
            if (preg_match('~maria|mysq~i', $driverName)) {
                $driverName = self::DRIVER_MYSQL;
            } elseif (preg_match('~postg|pg?sql~i', $driverName)) {
                $driverName = self::DRIVER_PGSQL;
            } elseif (strpos($driverName, 'sqlit') !== false) {
                $driverName = self::DRIVER_SQLITE;
            } elseif (strpos($driverName, 'oci') !== false) {
                $driverName = self::DRIVER_OCI8;
            } elseif (strpos($driverName, 'drizz') !== false) {
                $driverName = self::DRIVER_DRIZZLE;
            } elseif (preg_match('~ibm|db2~i', $driverName)) {
                $driverName = self::DRIVER_DB2;
            } elseif (preg_match('~mssql|sqlsrv~i', $driverName)) {
                $driverName = self::DRIVER_SQLSRV;
            }

            if (in_array($driverName, DriverManager::getAvailableDrivers())) {
                return $driverName;
            }
        }

        return false;
    }

    /**
     * Getting Doctrine Connection
     *
     * @return Connection
     */
    public function getConnection() : Connection
    {
        return $this->currentConnection;
    }

    /**
     * Get Table Prefix
     *
     * @return string
     */
    public function getTablePrefix() : string
    {
        return $this->currentTablePrefix;
    }

    /**
     * Get identifier
     *
     * @return string
     */
    public function getQuoteIdentifier() : string
    {
        return $this->currentQuoteIdentifier;
    }

    /**
     * Get user params
     *
     * @return array
     */
    public function getUserParams() : array
    {
        return $this->currentUserParams;
    }

    /**
     * Get Connection params
     *
     * @return array
     */
    public function getConnectionParams() : array
    {
        return $this->getParams();
    }

    /**
     * Trimming table for safe usage
     *
     * @param mixed $table
     * @return mixed
     */
    public function trimTableSelector($table)
    {
        if (is_array($table)) {
            foreach ($table as $key => $value) {
                $table[$key] = $this->trimTableSelector($value);
            }
            return $table;
        } elseif (is_object($table)) {
            foreach (get_object_vars($table) as $key => $value) {
                $table->{$key} = $this->trimTableSelector($value);
            }
            return $table;
        }
        if (is_string($table)) {
            $tableArray = explode('.', $table);
            foreach ($tableArray as $key => $value) {
                $tableArray[$key] = trim(
                    trim(
                        trim($value),
                        $this->currentQuoteIdentifier
                    )
                );
            }
            $table = implode('.', $tableArray);
        }

        return $table;
    }

    /**
     * Alternative multi variable type quoted identifier
     *
     * @param mixed $quoteStr
     * @return mixed
     * @throws \InvalidArgumentException
     */
    public function quoteIdentifiers($quoteStr)
    {
        if ($quoteStr instanceof \Closure || is_resource($quoteStr)) {
            throw new \InvalidArgumentException(
                "Invalid value to be quote, quote value could not be instance of `Closure` or as a `Resource`",
                E_USER_ERROR
            );
        }

        $quoteStr = $this->trimTableSelector($quoteStr);
        if (is_array($quoteStr)) {
            foreach ($quoteStr as $key => $value) {
                $quoteStr[$key] = $this->quoteIdentifiers($value);
            }
            return $quoteStr;
        } elseif (is_object($quoteStr)) {
            foreach (get_object_vars($quoteStr) as $key => $value) {
                $quoteStr->{$key} = $this->quoteIdentifiers($value);
            }
            return $quoteStr;
        }

        $return = $this->quoteIdentifier($quoteStr);

        return $return;
    }

    /**
     * Alternative multi variable type quote string
     *      Nested quotable
     *
     * @param mixed $quoteStr
     * @param int   $type
     * @return array|mixed|string
     */
    public function quotes($quoteStr, $type = \PDO::PARAM_STR)
    {
        if ($quoteStr instanceof \Closure || is_resource($quoteStr)) {
            throw new \InvalidArgumentException(
                "Invalid value to be quote, quote value could not be instance of `Closure` or as a `Resource`",
                E_USER_ERROR
            );
        }

        $quoteStr = $this->trimTableSelector($quoteStr);
        if (is_array($quoteStr)) {
            foreach ($quoteStr as $key => $value) {
                $quoteStr[$key] = $this->quotes($value, $type);
            }
            return $quoteStr;
        } elseif (is_object($quoteStr)) {
            foreach (get_object_vars($quoteStr) as $key => $value) {
                $quoteStr->{$key} = $this->quotes($value, $type);
            }
            return $quoteStr;
        }

        return $this->quote($quoteStr);
    }

    /**
     * Prefix CallBack
     *
     * @access private
     * @param  string $table the table
     * @return string
     */
    private function prefixTableCallback(string $table) : string
    {
        $prefix = $this->getTablePrefix();
        if (!empty($prefix) && is_string($prefix) && trim($prefix)) {
            $table = (strpos($table, $prefix) === 0)
                ? $table
                : $prefix.$table;
        }

        return $table;
    }

    /**
     * Prefixing table with predefined table prefix on configuration
     *
     * @param mixed $table
     * @param bool  $use_identifier
     * @return array|null|string
     */
    public function prefixTables($table, bool $use_identifier = false)
    {
        if ($table instanceof \Closure || is_resource($table)) {
            throw new \InvalidArgumentException(
                "Invalid value to be quote, table value could not be instance of `Closure` or as a `Resource`",
                E_USER_ERROR
            );
        }

        $prefix = $this->getTablePrefix();
        if (is_array($table)) {
            foreach ($table as $key => $value) {
                $table[$key] = $this->prefixTables($value, $use_identifier);
            }
            return $table;
        }
        if (is_object($table)) {
            foreach (get_object_vars($table) as $key => $value) {
                $table->{$key} = $this->prefixTables($value, $use_identifier);
            }
            return $table;
        }
        if (!is_string($table)) {
            return null;
        }
        if (strpos($table, $this->currentQuoteIdentifier) !== false) {
            $use_identifier = true;
        }
        if (!empty($prefix) && is_string($prefix) && trim($prefix)) {
            $tableArray = explode('.', $table);
            $tableArray    = $this->trimTableSelector($tableArray);
            if (count($tableArray) > 1) {
                $connectionParams = $this->getConnectionParams();
                if (isset($connectionParams['dbname']) && $tableArray[0] == $connectionParams['dbname']) {
                    $tableArray[1] = $this->prefixTableCallback($tableArray);
                }
                if ($use_identifier) {
                    return $this->currentQuoteIdentifier
                           . implode("{$this->currentQuoteIdentifier}.{$this->currentQuoteIdentifier}", $tableArray)
                           . $this->currentQuoteIdentifier;
                } else {
                    return implode(".", $tableArray);
                }
            } else {
                $table = $this->prefixTableCallback($tableArray[0]);
            }
        }

        return $use_identifier
            ? $this->currentQuoteIdentifier.$table.$this->currentQuoteIdentifier
            : $table;
    }

    /**
     * @uses Database::prefixTables()
     *
     * @param mixed $tables
     * @param bool  $use_identifier
     * @return mixed
     */
    public function prefix($tables, bool $use_identifier = false)
    {
        return $this->prefixTables($tables, $use_identifier);
    }

    /**
     * Compile Bindings
     *     Take From CI 3 Database Query Builder, default string Binding use Question mark ( ? )
     *
     * @param   string $sql   sql statement
     * @param   array  $binds array of bind data
     * @return  mixed
     */
    public function compileBindsQuestionMark(string $sql, $binds = null)
    {
        if (empty($binds) || strpos($sql, '?') === false) {
            return $sql;
        } elseif (! is_array($binds)) {
            $binds = [$binds];
            $bind_count = 1;
        } else {
            // Make sure we're using numeric keys
            $binds = array_values($binds);
            $bind_count = count($binds);
        }
        // Make sure not to replace a chunk inside a string that happens to match the bind marker
        if ($countMatches = preg_match_all("/'[^']*'/i", $sql, $matches)) {
            $countMatches = preg_match_all(
                '/\?/i', # regex
                str_replace(
                    $matches[0],
                    str_replace('?', str_repeat(' ', 1), $matches[0]),
                    $sql,
                    $countMatches
                ),
                $matches, # matches
                PREG_OFFSET_CAPTURE
            );
            // Bind values' count must match the count of markers in the query
            if ($bind_count !== $countMatches) {
                return false;
            }
        } elseif (($countMatches = preg_match_all('/\?/i', $sql, $matches, PREG_OFFSET_CAPTURE)) !== $bind_count) {
            return $sql;
        }

        do {
            $countMatches--;
            $escapedValue = is_int($binds[$countMatches])
                ? $binds[$countMatches]
                : $this->quote($binds[$countMatches]);
            if (is_array($escapedValue)) {
                $escapedValue = '('.implode(',', $escapedValue).')';
            }
            $sql = substr_replace($sql, $escapedValue, $matches[0][$countMatches][1], 1);
        } while ($countMatches !== 0);

        return $sql;
    }

    /**
     * Query using binding optionals statements
     *
     * @uses   compileBindsQuestionMark
     * @param  string $sql
     * @param  mixed  $statement array|string|null
     * @return Statement
     * @throws DBALException
     */
    public function queryBind(string $sql, $statement = null)
    {
        $sql = $this->compileBindsQuestionMark($sql, $statement);
        if ($sql === false) {
            throw new DBALException(
                sprintf(
                    'Invalid statement binding count with sql query : %s',
                    $sql
                ),
                E_USER_WARNING
            );
        }

        return $this->query($sql);
    }

    /**
     * Prepare & Execute directly
     *
     * @param string $query
     * @param array $bind
     *
     * @return Statement
     */
    public function executePrepare(string $query, array $bind = []) : Statement
    {
        $stmt = $this->prepare($query);
        $stmt->execute($bind);
        return $stmt;
    }

    /**
     * --------------------------------------------------------------
     * SCHEMA
     *
     * Lists common additional Methods just for check & lists only
     * to use more - please @uses Database::getSchemaManager()
     *
     * @see AbstractSchemaManager
     *
     * ------------------------------------------------------------ */

    /**
     * Check Table Maybe Invalid
     *
     * @param string $tableName
     * @return string
     * @throws \InvalidArgumentException
     */
    protected function tableMaybeInvalid($tableName) : string
    {
        if (!is_string($tableName)) {
            throw new \InvalidArgumentException(
                'Invalid table name type. Table name must be as string',
                E_USER_ERROR
            );
        }

        $tableName = trim($tableName);
        if ($tableName == '') {
            throw new \InvalidArgumentException(
                'Invalid parameter table name. Table name could not be empty.',
                E_USER_ERROR
            );
        }

        return $tableName;
    }

    /**
     * Get List Available Databases
     *
     * @return array
     */
    public function getListDatabases() : array
    {
        return $this->getSchemaManager()->listDatabases();
    }

    /**
     * Alias @uses getListDatabases()
     *
     * @return array
     */
    public function getListDatabase() : array
    {
        return $this->getListDatabases();
    }

    /**
     * Alias @uses getListDatabases()
     *
     * @return array
     */
    public function getDatabases() : array
    {
        return $this->getListDatabases();
    }

    /**
     * Alias @uses getListDatabases()
     *
     * @return array
     */
    public function listDatabases() : array
    {
        return $this->getListDatabases();
    }

    /**
     * Returns a list of all namespaces in the current database.
     *
     * @return array
     */
    public function getListNamespaceNames() : array
    {
        return $this->getSchemaManager()->listNamespaceNames();
    }

    /**
     * Alias @uses getListNamespaceNames()
     *
     * @return array
     */
    public function getListNamespaceName() : array
    {
        return $this->getListNamespaceNames();
    }

    /**
     * Alias @uses getListNamespaceNames()
     *
     * @return array
     */
    public function getNamespaceNames() : array
    {
        return $this->getListNamespaceNames();
    }

    /**
     * Alias @uses getListNamespaceNames()
     *
     * @return array
     */
    public function listNamespaceNames() : array
    {
        return $this->getListNamespaceNames();
    }

    /**
     * Lists the available sequences for this connection.
     *
     * @return Sequence[]
     */
    public function getSequences() : array
    {
        return $this->getSchemaManager()->listSequences();
    }

    /**
     * alias @uses getSequences()
     *
     * @return Sequence[]
     */
    public function getListSequences() : array
    {
        return $this->getSequences();
    }

    /**
     * alias @uses getSequences()
     *
     * @return Sequence[]
     */
    public function getListSequence() : array
    {
        return $this->getSequences();
    }

    /**
     * alias @uses getSequences()
     *
     * @return Sequence[]
     */
    public function listSequences() : array
    {
        return $this->getSequences();
    }

    /**
     * Get Doctrine Column of table
     *
     * @param string $tableName
     * @return Column[]
     */
    public function getTableColumns(string $tableName) : array
    {
        $tableName = $this->tableMaybeInvalid($tableName);
        return $this
            ->getSchemaManager()
            ->listTableColumns($tableName);
    }

    /**
     * aliases @uses getTableColumns()
     *
     * @param string $tableName
     * @return Column[]
     */
    public function getListTableColumn(string $tableName) : array
    {
        return $this->getTableColumns($tableName);
    }

    /**
     * aliases @uses getTableColumns()
     *
     * @param string $tableName
     * @return Column[]
     */
    public function getListTableColumns(string $tableName) : array
    {
        return $this->getTableColumns($tableName);
    }

    /**
     * aliases @uses getTableColumns()
     *
     * @param string $tableName
     * @return Column[]
     */
    public function listTableColumns(string $tableName) : array
    {
        return $this->getTableColumns($tableName);
    }

    /**
     * Lists the indexes for a given table returning an array of Index instances.
     *
     * Keys of the portable indexes list are all lower-cased.
     *
     * @param string $tableName The name of the table.
     *
     * @return Index[]
     */
    public function getListTableIndexes(string $tableName) : array
    {
        $tableName = $this->tableMaybeInvalid($tableName);
        return $this
            ->getSchemaManager()
            ->listTableIndexes($tableName);
    }

    /**
     * alias @uses getListTableIndexes()
     *
     * @param string $tableName
     *
     * @return Index[]
     */
    public function getTableIndexes(string $tableName) : array
    {
        return $this->getListTableIndexes($tableName);
    }

    /**
     * alias @uses getListTableIndexes()
     *
     * @param string $tableName
     *
     * @return Index[]
     */
    public function getListTableIndex(string $tableName) : array
    {
        return $this->getListTableIndexes($tableName);
    }

    /**
     * alias @uses getListTableIndexes()
     *
     * @param string $tableName
     *
     * @return Index[]
     */
    public function listTableIndexes(string $tableName) : array
    {
        return $this->getListTableIndexes($tableName);
    }

    /**
     * Check if table is Exists
     *
     * @param string $tables
     * @return bool
     * @throws \InvalidArgumentException
     */
    public function tablesExist($tables)
    {
        if (! is_string($tables) && !is_array($tables)) {
            throw new \InvalidArgumentException(
                'Invalid table name type. Table name must be as string or array',
                E_USER_ERROR
            );
        }

        $tables = $this->prefixTables($tables);
        ! is_array($tables) && $tables = [$tables];
        return $this
            ->getSchemaManager()
            ->tablesExist($tables);
    }

    /**
     * Returns a list of all tables in the current Database Connection.
     *
     * @return array
     */
    public function getListTableNames() : array
    {
        return $this
            ->getSchemaManager()
            ->listTableNames();
    }

    /**
     * Alias @uses getListTableNames()
     *
     * @return array
     */
    public function getTableNames() : array
    {
        return $this->getListTableNames();
    }

    /**
     * Alias @uses getListTableNames()
     *
     * @return array
     */
    public function getListTableName() : array
    {
        return $this->getListTableNames();
    }

    /**
     * Alias @uses getListTableNames()
     *
     * @return array
     */
    public function listTableNames() : array
    {
        return $this->getListTableNames();
    }

    /**
     * Get List Tables
     *
     * @return Table[]
     */
    public function getListTables() : array
    {
        return $this
            ->getSchemaManager()
            ->listTables();
    }

    /**
     * Alias @uses getListTables()
     *
     * @return Table[]
     */
    public function getListTable() : array
    {
        return $this->getListTables();
    }

    /**
     * Alias @uses getListTables()
     *
     * @return Table[]
     */
    public function listTables() : array
    {
        return $this->getListTables();
    }

    /**
     * Get Object Doctrine Table from Table Name
     *
     * @param string $tableName
     *
     * @return Table
     */
    public function getTableDetails(string $tableName) : Table
    {
        $tableName = $this->tableMaybeInvalid($tableName);
        return $this->getSchemaManager()->listTableDetails($tableName);
    }

    /**
     * Alias @uses getTableDetails()
     *
     * @param string $tableName
     *
     * @return Table
     */
    public function getListTableDetails(string $tableName) : Table
    {
        return $this->getTableDetails($tableName);
    }

    /**
     * Alias @uses getTableDetails()
     *
     * @param string $tableName
     *
     * @return Table
     */
    public function getListTableDetail(string $tableName) : Table
    {
        return $this->getTableDetails($tableName);
    }

    /**
     * Alias @uses getTableDetails()
     *
     * @param string $tableName
     *
     * @return Table
     */
    public function listTableDetails(string $tableName) : Table
    {
        return $this->getTableDetails($tableName);
    }

    /**
     * Lists the views this connection has.
     *
     * @return View[]
     */
    public function listViews() : array
    {
        return $this->getSchemaManager()->listViews();
    }

    /**
     * Get List views, alias @uses listViews()
     *
     * @return View[]
     */
    public function getListViews() : array
    {
        return $this->listViews();
    }

    /**
     * Get List views, alias @uses listViews()
     *
     * @return View[]
     */
    public function getListView() : array
    {
        return $this->listViews();
    }

    /**
     * Lists the foreign keys for the given table.
     *
     * @param string      $tableName    The name of the table.
     *
     * @return ForeignKeyConstraint[]
     */
    public function getTableForeignKeys(string $tableName) : array
    {
        $tableName = $this->tableMaybeInvalid($tableName);
        return $this->getSchemaManager()->listTableForeignKeys($tableName);
    }

    /**
     * Alias @uses getTableForeignKeys()
     *
     * @param string $tableName
     *
     * @return ForeignKeyConstraint[]
     */
    public function getListTableForeignKeys(string $tableName) : array
    {
        return $this->getTableForeignKeys($tableName);
    }

    /**
     * Alias @uses getTableForeignKeys()
     *
     * @param string $tableName
     *
     * @return ForeignKeyConstraint[]
     */
    public function getListTableForeignKey(string $tableName) : array
    {
        return $this->getTableForeignKeys($tableName);
    }

    /**
     * Alias @uses getTableForeignKeys()
     *
     * @param string $tableName
     *
     * @return ForeignKeyConstraint[]
     */
    public function listTableForeignKeys(string $tableName) : array
    {
        return $this->getTableForeignKeys($tableName);
    }

    /**
     * Magic Method __call - calling arguments for backward compatibility
     *
     * @uses Connection
     *
     * @param string $method method object :
     *                       @see Connection
     * @param array  $arguments the arguments list
     * @return mixed
     * @throws DBALException
     */
    public function __call(string $method, array $arguments)
    {
        /**
         * check if method exists on connection @see Connection !
         */
        if (method_exists($this->getConnection(), $method)) {
            return call_user_func_array([$this->getConnection(), $method], $arguments);
        }

        throw new \BadMethodCallException(
            sprintf(
                "Call to undefined Method %s",
                $method
            ),
            E_USER_ERROR
        );
    }
}
