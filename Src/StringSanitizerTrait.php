<?php
declare(strict_types=1);

namespace Pentagonal\DatabaseDBAL;

/**
 * Trait StringSanitizerTrait
 * @package Pentagonal\DatabaseDBAL
 */
trait StringSanitizerTrait
{
    /**
     * Sanitize Result to UTF-8 , this is recommended to sanitize
     * that result from socket that invalid decode UTF8 values
     *
     * @param string $string
     *
     * @return string
     */
    public function sanitizeInvalidUTF8(string $string) : string
    {
        if (!function_exists('iconv')) {
            return $string;
        }

        if (! function_exists('mb_strlen') || mb_strlen($string, 'UTF-8') !== strlen($string)) {
            // add temporary error handler
            set_error_handler(function ($errNo, $errStr) {
                throw new \Exception(
                    $errStr,
                    $errNo
                );
            });
            $result = false;
            // try to un-serial
            try {
                /**
                 * use trim if possible
                 * Serialized value could not start & end with white space
                 */
                $result = iconv('windows-1250', 'UTF-8//IGNORE', $string);
            } catch (\Exception $e) {
                // pass
            }

            restore_error_handler();
            if ($result !== false) {
                return $result;
            }
        }

        return $string;
    }

    /* --------------------------------------------------------------------------------*
     |                              Serialize Helper                                   |
     |                                                                                 |
     | Custom From WordPress Core wp-includes/functions.php                            |
     |---------------------------------------------------------------------------------|
     */

    /**
     * Check value to find if it was serialized.
     * If $data is not an string, then returned value will always be false.
     * Serialized data is always a string.
     *
     * @param  mixed $data   Value to check to see if was serialized.
     * @param  bool  $strict Optional. Whether to be strict about the end of the string. Defaults true.
     * @return bool  false if not serialized and true if it was.
     */
    public function isSerialized($data, $strict = true)
    {
        /* if it isn't a string, it isn't serialized
         ------------------------------------------- */
        if (! is_string($data) || trim($data) == '') {
            return false;
        }

        $data = trim($data);
        // null && boolean
        if ('N;' == $data || $data == 'b:0;' || 'b:1;' == $data) {
            return true;
        }

        if (strlen($data) < 4 || ':' !== $data[1]) {
            return false;
        }

        if ($strict) {
            $last_char = substr($data, -1);
            if (';' !== $last_char && '}' !== $last_char) {
                return false;
            }
        } else {
            $semicolon = strpos($data, ';');
            $brace     = strpos($data, '}');

            // Either ; or } must exist.
            if (false === $semicolon && false === $brace
                || false !== $semicolon && $semicolon < 3
                || false !== $brace && $brace < 4
            ) {
                return false;
            }
        }

        $token = $data[0];
        switch ($token) {
            /** @noinspection PhpMissingBreakStatementInspection */
            case 's':
                if ($strict) {
                    if ('"' !== substr($data, -2, 1)) {
                        return false;
                    }
                } elseif (false === strpos($data, '"')) {
                    return false;
                }
            // or else fall through
            case 'a':
            case 'O':
            case 'C':
                return (bool) preg_match("/^{$token}:[0-9]+:/s", $data);
            case 'i':
            case 'd':
                $end = $strict ? '$' : '';
                return (bool) preg_match("/^{$token}:[0-9.E-]+;$end/", $data);
        }

        return false;
    }

    /**
     * Un-serialize value only if it was serialized.
     *
     * @param  string $original Maybe un-serialized original, if is needed.
     * @return mixed  Un-serialized data can be any type.
     */
    public function maybeUnSerialize($original)
    {
        if (! is_string($original) || trim($original) == '') {
            return $original;
        }

        /**
         * Check if serialized
         * check with trim
         */
        if ($this->isSerialized($original)) {
            // add temporary error handler
            set_error_handler(function ($errNo, $errStr) {
                throw new \Exception(
                    $errStr,
                    $errNo
                );
            });
            // try to un-serial
            try {
                /**
                 * use trim if possible
                 * Serialized value could not start & end with white space
                 */
                $original = @unserialize(trim($original));
            } catch (\Exception $e) {
                // pass
            }

            restore_error_handler();
        }

        return $original;
    }

    /**
     * Serialize data, if needed. @uses for ( un-compress serialize values )
     * This method to use safe as save data on database. Value that has been
     * Serialized will be double serialize to make sure data is stored as original
     *
     *
     * @param  mixed $data            Data that might be serialized.
     * @param  bool  $doubleSerialize Double Serialize if want to use returning real value of serialized
     *                                for database result
     * @return mixed A scalar data
     */
    public function maybeSerialize($data, $doubleSerialize = false)
    {
        if (is_array($data) || is_object($data)) {
            return @serialize($data);
        }

        /**
         * Double serialization is required for backward compatibility.
         * if @param bool $doubleSerialize is enabled
         */
        if ($doubleSerialize && $this->isSerialized($data, false)) {
            return serialize($data);
        }

        return $data;
    }
}
