function x() {
	console.log('HERE:');
	console.log(document.getElementById('file').files[0]['name']);
	console.log(document.getElementById('file').files.length);

	// $.post('/loaded/search/', {
	// 	files: document.getElementById('file').files[0]['name']
	// });
}

function validateForm() {
  var x = document.forms["myForm"]["term"].value;
  if (x == "") {
    alert("Name must be filled out");
    return false;
  }
}

// setup load event
// window.addEventListener("load", x, true);