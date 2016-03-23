<?php
// Run this test case with:
// phpunit csvdbtest.php

require '../csvdb.php';

class CsvDbTest extends PHPUnit_Framework_TestCase
{
    protected $filename;
        
    protected function tearDown()
    {
        // if (file_exists($this->filename)) {
        //     unlink($this->filename);
        // }
    }
    
    protected function setUp()
    {
        $this->filename = dirname(__FILE__) . '/test.csv';
        if (file_exists($this->filename)) {
            unlink($this->filename);
        }
    }

    public function testCreateTable()
    {
        $result = csvdbCreateTable($this->filename, array(array('name'=>'street'),array('name'=>'zip','constraint'=>'NOT NULL')));
        $this->assertSame($result['code'], 201, $result['value']);
        $this->assertFileExists($this->filename);

        // table already exists
        $result = csvdbCreateTable($this->filename, array(array('name'=>'street'),array('name'=>'zip','constraint'=>'NOT NULL')));
        $this->assertSame($result['code'], 409, $result['value']);
        $this->assertTrue(unlink($this->filename));

        // bad constraint
        $result = csvdbCreateTable($this->filename, array(array('name'=>'street'),array('name'=>'zip','constraint'=>'BLAH')));
        $this->assertSame($result['code'], 400, $result['value']);
        $this->assertFileNotExists($this->filename);
        
        // no name in schema field
        $result = csvdbCreateTable($this->filename, array(array('name'=>'street'),array('noname'=>'zip','constraint'=>'BLAH')));
        $this->assertSame($result['code'], 500, $result['value']);
        $this->assertFileNotExists($this->filename);

        // no schema
        $result = csvdbCreateTable($this->filename, null);
        $this->assertSame($result['code'], 500, $result['value']);
        $this->assertFileNotExists($this->filename);
        $result = csvdbCreateTable($this->filename, array());
        $this->assertSame($result['code'], 500, $result['value']);
        $this->assertFileNotExists($this->filename);
    }

    public function testSelect()
    {
        $result = csvdbCreateTable($this->filename, [['name'=>'fname'],['name'=>'lname'],['name'=>'zip']]);
        $this->assertSame($result['code'], 201, $result['value']);
        $this->assertFileExists($this->filename);
        $result = csvdbAddRecord($this->filename, [['fname'=>'Barnaby'],['lname'=>'Falls'],['zip'=>'91234']]);
        $this->assertSame($result['code'], 201, $result['value']);
        $result = csvdbAddRecord($this->filename, [['fname'=>'Yulia'],['lname'=>'Falls'],['zip'=>'94321']]);
        $this->assertSame($result['code'], 201, $result['value']);
        $result = csvdbSelect($this->filename, ['fname']);
        var_dump($result);
        
    }
}
?>
