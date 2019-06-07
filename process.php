<?php

/* 
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

use Symfony\Component\Process\Exception\ProcessFailedException;
use Symfony\Component\Process\Process;

function create_process() {
    $process = new Process(['python', 'XMLRPC\\rpc.py']);
    $process->run();

    // executes after the command finishes
    if (!$process->isSuccessful()) {
        throw new ProcessFailedException($process);
    }

    echo $process->getOutput();
}
