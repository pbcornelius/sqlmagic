The block in custom.js needs to be added to "user/.jupyter/custom/custom.js".

If there are multiple SQL magics, a combined block can be used:

require(['notebook/js/codecell'], function(codecell) {
  codecell.CodeCell.options_default.highlight_modes['magic_text/x-mssql'] = {'reg':[/^%%[oj]?sql/]} ;
  Jupyter.notebook.events.one('kernel_ready.Kernel', function(){
  Jupyter.notebook.get_cells().map(function(cell){
      if (cell.cell_type == 'code'){ cell.auto_highlight(); } }) ;
  });
});