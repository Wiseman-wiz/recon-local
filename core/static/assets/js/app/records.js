var cardsToShow = 4

function showMore() {
    var listData = Array.prototype.slice
      .call(document.querySelectorAll(".show-hide-list .show-hide-item:not(.d-block)"))
      .slice(0, cardsToShow);
  
    for (var i = 0; i < listData.length; i++) {
      listData[i].className = "col-12 d-block show-hide-item";
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
      listData[i].className = "col-12 d-none show-hide-item";
    }
    switchButtons();
  }
  
  function switchButtons() {
    var hiddenElements = Array.prototype.slice.call(
      document.querySelectorAll(".show-hide-list .show-hide-item:not(.d-block)")
    );
    if (hiddenElements.length == 0) {
        var x = document.getElementsByClassName("btn-show-more")
        var i;
        for (i = 0; i < x.length; i++) {
          x[i].style.display = 'none';
        }
    } else {
        var y = document.getElementsByClassName("btn-show-more")
        var i;
        for (i = 0; i < y.length; i++) {
            y[i].style.display = 'block';
        }
    }
  
    var shownElements = Array.prototype.slice.call(
      document.querySelectorAll(".show-hide-list .show-hide-item:not(.d-none)")
    );
    if (shownElements.length == 0 || shownElements.length <= cardsToShow) {
        var x = document.getElementsByClassName("btn-show-less")
        var i;
        for (i = 0; i < x.length; i++) {
          x[i].style.display = 'none';
        }
    } else {
        var y = document.getElementsByClassName("btn-show-less")
        var i;
        for (i = 0; i < y.length; i++) {
            y[i].style.display = 'block';
        }
    }
  }
  
  
  onload = function () {
    showMore();
  };

  var search_input = document.querySelector("#search-filter");

  var filter_reports = function(e){
    var report_list = document.querySelector("#rendered-list");
    const term = e.target.value.toLowerCase();
    const report_cards = report_list.getElementsByTagName("section")
    if (term.length > 0) {
        document.querySelector(".btn-show-less").classList.add("d-none")
        document.querySelector(".btn-show-more").classList.add("d-none")

        Array.from(document.querySelectorAll(".show-hide-item:not(.d-block)"))
            .forEach(function(val) {
                val.classList.remove('d-none');
                val.classList.add('d-block');
                console.log(val)
                
        });
    } else {
        document.querySelector(".btn-show-less").classList.remove("d-none")
        document.querySelector(".btn-show-more").classList.remove("d-none")

        Array.from(document.querySelectorAll(".show-hide-item"))
            .forEach(function(val) {
                val.classList.remove('d-block');
                val.classList.add('d-none');
        });
        showMore();
    }
    Array.from(report_cards).forEach(function(report){
        if (report.id.toLowerCase().indexOf(term) != -1) {
            report.classList.add("d-block")
            report.classList.remove("d-none")
        } else {
            report.classList.add("d-none")
            report.classList.remove("d-block")
        }
    })
}


search_input.addEventListener('keyup', filter_reports);