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

function fetchResults(query) {

  let data = {"type":"ranked_bm", "query":(query.value)};

  fetch("http://34.122.121.173:5001/search ", {
    method: "POST",
    headers: {}, 
    body: JSON.stringify(data)
  }).then(res => res.text())
    .then(text => document.getElementById("results").textContent = text);
    
    // console.log(res.text());
    // let text = res.json();
    // console.log(res.text());
    // 
  
}

function mockQuery()
{
  console.log("mock is ok");
  let data = {"type": "ranked_bm", "query":"love"};

  fetch("http://34.122.121.173:5001/search ", {
    method: "POST",
    headers: {}, 
    body: JSON.stringify(data)
  }).then(res => {
    console.log(res.text());
  });
  
}

console.log("hi");
mockQuery();  