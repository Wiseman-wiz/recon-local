var cardsToShow = 12

function showMore() {
    var listData = Array.prototype.slice
      .call(document.querySelectorAll(".show-hide-list .show-hide-item:not(.d-block)"))
      .slice(0, cardsToShow);
  
    for (var i = 0; i < listData.length; i++) {
      listData[i].className = "col-xl-3 col-md-3 col-sm-6 d-block show-hide-item";
    }
    switchButtons();
  }
  
  function showLess() {
      var listNodes = document.querySelectorAll(".show-hide-list .show-hide-item:not(.d-none)")
      if (listNodes.length < cardsToShow * 2 && listNodes.length > cardsToShow) {
          var listData = Array.prototype.slice
          .call(listNodes)
          .slice(-(listNodes.length % cardsToShow))
      } else {
          var listData = Array.prototype.slice
          .call(listNodes)
          .slice(-cardsToShow)
      }
      
    for (var i = 0; i < listData.length; i++) {
    listData[i].className = "col-xl-3 col-md-3 col-sm-6 d-none show-hide-item";
    }
    switchButtons();
  }
  
  function switchButtons() {
    var hiddenElements = Array.prototype.slice.call(
      document.querySelectorAll(".show-hide-list .show-hide-item:not(.d-block)")
    );
    if (hiddenElements.length == 0) {
      document.getElementById("moreButton").style.display = "none";
    } else {
      document.getElementById("moreButton").style.display = "block";
    }
  
    var shownElements = Array.prototype.slice.call(
      document.querySelectorAll(".show-hide-list .show-hide-item:not(.d-none)")
    );
    if (shownElements.length == 0 || shownElements.length <= cardsToShow) {
      document.getElementById("lessButton").style.display = "none";
    } else {
      document.getElementById("lessButton").style.display = "block";
    }
  }
  
  onload = function () {
    showMore();
  };
  

// $(document).ready(function () {
//   n = $(".show-hide-list .show-hide-item").length;
//   x=8;
//   $('.show-hide-list .show-hide-item:lt('+x+')').show();
//   $('#moreButton').click(function () {
//       x= (x+8 <= n) ? x+8 : n;
//       $('.show-hide-list .show-hide-item:lt('+x+')').show();
//        $('#lessButton').show();
//       if(x == n){
//           $('#moreButton').hide();
//       }
//   });
//   $('#lessButton').click(function () {
//       x=(x-8<0) ? 8 : x-8;
//       $('.show-hide-list .show-hide-item').not(':lt('+x+')').hide();
//       $('#moreButton').show();
//        $('#lessButton').show();
//       if(x == 8){
//           $('#lessButton').hide();
//       }
//   });
// });