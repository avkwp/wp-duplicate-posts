/* 
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */


function onClickAttributes() {
    document.getElementById("textarea_query").style.display = "none";
    document.getElementById("textarea_csv").style.display = "none";
}

function onClickTextArea() {
    document.getElementById("textarea_csv").style.display = "block";
    document.getElementById("textarea_query").style.display = "none";
}

function onClickQuery() {
    document.getElementById("textarea_csv").style.display = "none";
    document.getElementById("textarea_query").style.display = "block";
}