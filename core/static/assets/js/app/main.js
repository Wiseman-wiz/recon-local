$(function () {
    CodeMirror.fromTextArea(document.getElementById("codeMirrorInit"), {
      mode: "python",
      theme: "monokai",
      keyMap: "sublime"
    });
})

var toggleSwitch = document.querySelector('.theme-switch input[type="checkbox"]');
var currentTheme = localStorage.getItem('theme');
var mainHeader = document.querySelector('.main-header');

if (currentTheme) {
    if (currentTheme === 'dark') {
        if (!document.body.classList.contains('dark-mode')) {
            document.body.classList.add("dark-mode");
        }
        if (mainHeader.classList.contains('navbar-light')) {
            mainHeader.classList.add('navbar-dark');
            mainHeader.classList.remove('navbar-light');
        }
        toggleSwitch.checked = true;
    }
}

function switchTheme(e) {
if (e.target.checked) {
    if (!document.body.classList.contains('dark-mode')) {
        document.body.classList.add("dark-mode");
    }
    if (mainHeader.classList.contains('navbar-light')) {
        mainHeader.classList.add('navbar-dark');
        mainHeader.classList.remove('navbar-light');
    }
    localStorage.setItem('theme', 'dark');
} else {
    if (document.body.classList.contains('dark-mode')) {
        document.body.classList.remove("dark-mode");
    }
    if (mainHeader.classList.contains('navbar-dark')) {
        mainHeader.classList.add('navbar-light');
        mainHeader.classList.remove('navbar-dark');
    }
    localStorage.setItem('theme', 'light');
}
}

toggleSwitch.addEventListener('change', switchTheme, false);

$(document).ready(function () {

    $(window).scroll(function() {
      if ($(window).scrollTop() >= 300) {
        $('#go-top-btn').addClass('show');
      } else {
        $('#go-top-btn').removeClass('show');
      }
    });
  
    $('#go-top-btn').click(function () {
      $('html, body').animate({scrollTop: 0}, '300');
    });
});


/** add active class and stay opened when selected */
var url = window.location;

// for sidebar menu entirely but not cover treeview
$('ul.nav-sidebar a').filter(function() {
return this.href == url;
}).addClass('active');

// for treeview
$('ul.nav-treeview a').filter(function() {
return this.href == url;
}).parentsUntil(".nav-sidebar > .nav-treeview").addClass('menu-open').prev('a').addClass('active');