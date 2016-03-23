<?php
// Run this test case with:
// phpunit csvdbtest.php

require '../csvdb.php';
// require '../myTestFunction.php';

class CsvDbTest extends PHPUnit_Framework_TestCase
{
    protected $filename;
        
    protected function tearDown()
    {
        if (file_exists($this->filename)) {
            unlink($this->filename);
        }
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
        // $b = myTestFunction($filename, array(array('name'=>'street'),array('name'=>'zip','constraint'=>'NOT NULL')));
        // $this->assertTrue($b);
        $result = csvdbCreateTable($this->filename, array(array('name'=>'street'),array('name'=>'zip','constraint'=>'NOT NULL')));
        $this->assertSame($result['code'], 201, $result['value']);
        $this->assertFileExists($this->filename);

        $result = csvdbCreateTable($this->filename, array(array('name'=>'street'),array('name'=>'zip','constraint'=>'NOT NULL')));
        $this->assertSame($result['code'], 409, $result['value']);
        $this->assertTrue(unlink($this->filename));

        $result = csvdbCreateTable($this->filename, array(array('name'=>'street'),array('name'=>'zip','constraint'=>'BLAH')));
        $this->assertSame($result['code'], 400, $result['value']);
        $this->assertFileNotExists($this->filename);
    }
}
?>
