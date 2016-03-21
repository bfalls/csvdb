<?php
// Run this test case with:
// phpunit csvdbtest.php

require '../csvdb.php';
// require '../myTestFunction.php';

class CsvDbTest extends PHPUnit_Framework_TestCase
{
    public function testCreateTable()
    {
        $filename = dirname(__FILE__) . '/test.csv';
        // $b = myTestFunction($filename, array(array('name'=>'street'),array('name'=>'zip','constraint'=>'NOT NULL')));
        // $this->assertTrue($b);
        $result = csvdbCreateTable($filename, array(array('name'=>'street'),array('name'=>'zip','constraint'=>'NOT NULL')));
        $this->assertSame($result['code'], 201, $result['value']);
        $this->assertFileExists($filename);

        $result = csvdbCreateTable($filename, array(array('name'=>'street'),array('name'=>'zip','constraint'=>'NOT NULL')));
        $this->assertSame($result['code'], 409, $result['value']);
    }
}
?>
