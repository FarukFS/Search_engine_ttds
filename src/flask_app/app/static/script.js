function opentab(evt, tabName) {
  var i, tabcontent, tablinks;
  tabcontent = document.getElementsByClassName("tabcontent");
  for (i = 0; i < tabcontent.length; i++) {
    tabcontent[i].style.display = "none";
  }
  tablinks = document.getElementsByClassName("tablinks");
  for (i = 0; i < tablinks.length; i++) {
    tablinks[i].className = tablinks[i].className.replace(" active", "");
  }
  document.getElementById(tabName).style.display = "block";
  evt.currentTarget.className += " active";
}

function handleResults(text) {
  globalVal = text;
  // console.log("reaching");
  // let x = text;
  // window.onload = function() {
    if (window.jQuery) {  
      $('#pagination-container').pagination({
        dataSource: text.Results,
        callback: function(data, pagination) {
            var html = simpleTemplating(data);
            $('#data-container').html(html);
        }
      })
    } else {
        // jQuery is not loaded
        alert("No JQuery");
    }
}

let globalVal = null;

function fetchResults(query) {

  let data = {"type":"ranked", "query":(query.value)}; 

  fetch("http://34.122.121.173:5001/search ", {
    method: "POST",
    headers: {}, 
    body: JSON.stringify(data)
  }).then(res => res.json())
    .then(text => handleResults(text))
      
    
    // console.log(res.text());
    // let text = res.json();
    // console.log(res.text());
    // 
  
}

function mockQuery()
{
  console.log("mock is ok");
  let data = {"type": "boolean", "query":"love"}; 

  fetch("http://34.122.121.173:5001/search ", {
    method: "POST",
    headers: {}, 
    body: JSON.stringify(data)
  }).then(res => {
    console.log(res.text());
  });
  
}
mockQuery();  



function simpleTemplating(data) {
  var html = '<ul>';
  $.each(data, function(index, item){
      html += '<li>' + "<img style='width:100px' src='./static/testBuffalo.gif'></img>" +  item.Artist + " " + item.Title + '</li>';
  });
  html += '</ul>';
  return html;
}




function log(content) {
window.console && console.log(content);
}
