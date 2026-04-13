$(function() {

    $('#checkAllApproved').click(function() {
        if (this.checked) {
            $('.checkOneApproved').each(function(){
                this.checked = true;
            });
        } else {
            $('.checkOneApproved').each(function(){
                this.checked = false;
            });
        }
    });

    $('#checkAllForApproval').click(function() {
        if (this.checked) {
            $('.checkOneForApproval').each(function(){
                this.checked = true;
            });
        } else {
            $('.checkOneForApproval').each(function(){
                this.checked = false;
            });
        }
    });

    $('#checkAllNotApproved').click(function() {
        if (this.checked) {
            $('.checkOneNotApproved').each(function(){
                this.checked = true;
            });
        } else {
            $('.checkOneNotApproved').each(function(){
                this.checked = false;
            });
        }
    });
    
    
    //Initialize Select2 Elements
    $('.select2').select2()

    //Initialize Select2 Elements
    $('.select2bs4').select2({
      theme: 'bootstrap4',
    })

    $('[data-toggle="tooltip"]').tooltip()

    $(document).on('mouseenter', ".text-truncate", function () {
        var $this = $(this);
        if (this.offsetWidth < this.scrollWidth && !$this.attr('title')) {
            $this.tooltip({
                title: $this.text(),
                placement: "bottom"
            });
            $this.tooltip('show');
        }
    });
})