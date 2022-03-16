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

function mockQuery()
{
  console.log("mock is ok");
  let data = {"type": "boolean", "query":"love"}; 

  fetch("http://localhost:5001/search ", {
    method: "POST",
    headers: {}, 
    body: JSON.stringify(data)
  }).then(res => {
    console.log(res.text());
  });
  
}

console.log("hi");
mockQuery();  