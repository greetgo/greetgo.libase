package kz.greepto.gpen.editors.gpen

import java.util.ArrayList
import java.util.List
import kz.greepto.gpen.drawport.DrawPort
import kz.greepto.gpen.drawport.Kolor
import kz.greepto.gpen.drawport.Kursor
import kz.greepto.gpen.drawport.Rect
import kz.greepto.gpen.drawport.Vec2
import kz.greepto.gpen.drawport.swt.DrawPortSwt
import kz.greepto.gpen.drawport.swt.DrawableGcSource
import kz.greepto.gpen.editors.gpen.action.Oper
import kz.greepto.gpen.editors.gpen.action.UndoableOperation
import kz.greepto.gpen.editors.gpen.model.IdFigure
import kz.greepto.gpen.editors.gpen.model.Scene
import kz.greepto.gpen.editors.gpen.model.paint.PaintResult
import kz.greepto.gpen.editors.gpen.model.paint.SelChecker
import kz.greepto.gpen.editors.gpen.model.visitor.Hit
import kz.greepto.gpen.editors.gpen.model.visitor.VisitorPaint
import kz.greepto.gpen.editors.gpen.model.visitor.VisitorPlacer
import kz.greepto.gpen.editors.gpen.prop.PropFactory
import kz.greepto.gpen.editors.gpen.prop.SceneWorker
import kz.greepto.gpen.editors.gpen.style.dev.DevStyleCalc
import kz.greepto.gpen.util.ColorManager
import kz.greepto.gpen.util.CursorManager
import kz.greepto.gpen.util.FontManager
import kz.greepto.gpen.util.Handler
import kz.greepto.gpen.util.HandlerList
import org.eclipse.core.commands.operations.IUndoContext
import org.eclipse.core.commands.operations.OperationHistoryFactory
import org.eclipse.swt.SWT
import org.eclipse.swt.events.FocusEvent
import org.eclipse.swt.events.FocusListener
import org.eclipse.swt.events.MouseEvent
import org.eclipse.swt.events.MouseListener
import org.eclipse.swt.events.MouseMoveListener
import org.eclipse.swt.events.MouseTrackListener
import org.eclipse.swt.events.PaintEvent
import org.eclipse.swt.widgets.Canvas
import org.eclipse.swt.widgets.Composite

class GpenCanvas extends Canvas implements MouseListener, MouseMoveListener, MouseTrackListener {
  val MOVE_OFFSET = 3.0

  Scene originalScene = new Scene
  Scene scene = new Scene

  val colors = new ColorManager
  val fonts = new FontManager
  val styleCalc = new DevStyleCalc
  val cursors = new CursorManager
  val List<String> selIdList = new ArrayList

  val SelChecker selChecker = [ IdFigure figure |
    if(selIdList.contains(figure.id)) return true
    return figureIdsAtSelRect.contains(figure.id)
  ]

  package val SelectionProvider selectionProvider = new SelectionProvider(this)

  val HandlerList changeSceneHandlerList = new HandlerList

  val SceneWorker sceneWorker = new SceneWorker() {
    override takeId(Object object) {
      return (object as IdFigure).id
    }

    override applyOper(Oper oper) {
      if(oper === null) return;

      var op = new UndoableOperation(oper, scene) [
        redraw
        changeSceneHandlerList.fire
      ]
      op.addContext(undoContext)

      //var ophist = PlatformUI.getWorkbench().operationSupport.operationHistory
      var ophist = OperationHistoryFactory.getOperationHistory()
      if (ophist.getLimit(undoContext) < 2000) {
        ophist.setLimit(undoContext, 2000)
      }
      ophist.execute(op, null, null)

      redraw
    }

    override addChangeHandler(Handler handler) {
      return changeSceneHandlerList.add(handler)
    }
  }

  private Vec2 mouse = Vec2.from(0, 0)

  def void setScene(Scene scene) {
    originalScene = scene
    this.scene = originalScene.copy
  }

  val IUndoContext undoContext

  var hasFocus = false

  public new(Composite parent, IUndoContext undoContext) {
    super(parent, SWT.NONE);

    this.undoContext = undoContext

    addPaintListener [ PaintEvent e |
      paintCanvas(e);
    ];

    addMouseListener(this);
    addMouseMoveListener(this);
    addMouseTrackListener(this);

    new Thread(
      [
        while (!disposed) {
          Thread.sleep(1000 / 24)
          display.syncExec [
            if (!disposed && hasFocus) {
              redraw
            }
          ]
        }
      ]).start

    addFocusListener(
      new FocusListener() {
        override focusGained(FocusEvent e) {
          hasFocus = true
        }

        override focusLost(FocusEvent e) {
          hasFocus = false
        }
      })
  }

  def DrawPort createDP() { DrawPortSwt.fromGcCreator(new DrawableGcSource(fonts, colors, this)) }

  def paintCanvas(PaintEvent e) {
    paintScene()

    if (draggingPaintResult !== null) {
      var dp = createDP
      try {
        draggingPaintResult.paintDrag(dp, mouse)
      } finally {
        dp.dispose
      }
    }

    if(selectorFrom !== null) paintSelector
  }

  private def paintSelector() {
    if(selectorFrom === null) return
    var dp = createDP
    try {
      var r = Rect.fromTo(selectorFrom, mouse)

      if (selectorFrom.x < mouse.x) {
        dp.style.foreground = Kolor.GREEN

        val int step = 20
        val int period = 600
        var ofs = -((System.currentTimeMillis % period) as double / period ) * step
        var skvaj = 0.5

        var x = dp.from(r.rightBottom)
        ofs = x.to(r.leftBottom).dashLine(ofs, skvaj, step)
        ofs = x.to(r.leftTop).dashLine(ofs, skvaj, step)
        ofs = x.to(r.rightTop).dashLine(ofs, skvaj, step)
        ofs = x.to(r.rightBottom).dashLine(ofs, skvaj, step)

      } else {
        dp.style.foreground = Kolor.BLUE

        val int step = 20
        val int period = 600
        var ofs = -((System.currentTimeMillis % period) as double / period ) * step
        var skvaj = 0.5

        var x = dp.from(r.rightBottom)
        ofs = x.to(r.leftBottom).dashLine(ofs, skvaj, step)
        ofs = x.to(r.leftTop).dashLine(ofs, skvaj, step)
        ofs = x.to(r.rightTop).dashLine(ofs, skvaj, step)
        ofs = x.to(r.rightBottom).dashLine(ofs, skvaj, step)
      }

    } finally {
      dp.dispose
    }
  }

  private def PaintResult paintScene() {
    var dp = createDP
    try {
      var placer = new VisitorPlacer(dp, styleCalc, selChecker)
      var vp = new VisitorPaint(placer)
      vp.mouse = mouse.copy

      var ret = scene.visit(vp)

      if (draggingPaintResult === null) {
        displayCursor(ret?.kursor)
      } else {
        displayCursor(draggingPaintResult.kursor)
      }

      return ret
    } finally {
      dp.dispose
    }
  }

  private def displayCursor(Kursor kursor) {
    var cc = display.cursorControl
    if(cc === null) return;
    if (kursor === null) {
      cc.cursor = cursors.getCursor(Kursor.ARROW)
    } else {
      cc.cursor = cursors.getCursor(kursor)
    }
  }

  override mouseDoubleClick(MouseEvent e) {
    display.cursorControl.cursor = null //new Cursor(display, SWT.CURSOR_SIZEWE)
    println('--=-- DOUBLE <<' + e + '>> --=--')
  }

  Vec2 mouseDownedAt = null
  boolean dragging = false
  PaintResult draggingPaintResult = null

  Vec2 selectorFrom = null

  override mouseDown(MouseEvent e) {
    mouse.x = e.x
    mouse.y = e.y

    if (Mouse.LMB(e)) {

      var pr = paintScene()

      if (pr == null || !pr.hasOper) {
        selectorFrom = mouse.copy
        redraw
        return
      }

      draggingPaintResult = pr

      mouseDownedAt = mouse.copy

      dragging = false

      return
    }

    if (Mouse.Ctrl_LMB(e)) {

      var dp = createDP
      try {
        var placer = new VisitorPlacer(dp, styleCalc, selChecker)
        var hitted = Hit.on(scene).with(placer).to(mouse)
        if (hitted.size === 0) {
          selectorFrom = mouse.copy
          redraw
          return
        } else {
          hitted.forEach [
            if (selIdList.contains(id)) {
              selIdList.remove(id)
            } else {
              selIdList += id
            }
          ]
        }
      } finally {
        dp.dispose
      }

      updateSelectionProvider
      return
    }

  }

  override mouseMove(MouseEvent e) {
    mouse.x = e.x
    mouse.y = e.y
    redraw

    if (mouseDownedAt !== null) {
      if (!dragging && (mouse - mouseDownedAt).len >= MOVE_OFFSET) {
        dragging = true
        redraw
        return
      }

      return
    }
  }

  override mouseUp(MouseEvent e) {
    mouse.x = e.x
    mouse.y = e.y

    if (e.button === 1) {
      if (draggingPaintResult === null) {
        if (selectorFrom === null) {
          if(!Mouse.hasCtrl(e)) selectOne
          return
        }
        {
          var rectIds = figureIdsAtSelRect
          if (Mouse.hasCtrl(e)) {
            rectIds.forEach [
              if (selIdList.contains(it)) {
                selIdList.remove(it)
              } else {
                selIdList += it
              }
            ]
          } else {
            selIdList.clear
            selIdList += rectIds
          }
          selectorFrom = null
          updateSelectionProvider
          return
        }
      }
      {
        if (!dragging) {
          mouseDownedAt = null
          draggingPaintResult = null
          selectOne
          return
        }

        sceneWorker.applyOper(draggingPaintResult.createOper(mouse))

        mouseDownedAt = null
        draggingPaintResult = null
        dragging = false
        return
      }
    }
  }

  def List<String> getFigureIdsAtSelRect() {
    if(selectorFrom === null) return #[]
    val selRect = SelRect.from(selectorFrom, mouse)
    var dp = createDP
    try {
      val placer = new VisitorPlacer(dp, styleCalc, null)
      return scene.list.filter[selRect.at(visit(placer))].map[id].toList
    } finally {
      dp.dispose
    }
  }

  private def selectOne() {
    var dp = createDP
    try {
      var placer = new VisitorPlacer(dp, styleCalc, selChecker)
      selIdList.clear
      Hit.on(scene).with(placer).to(mouse).forEach[selIdList += id]
    } finally {
      dp.dispose
    }

    updateSelectionProvider
  }

  def IdFigure getTopSelected() {
    var lastId = selIdList.last
    if(lastId === null) return null
    return scene.findById(lastId)
  }

  def void updateSelectionProvider() {
    var sel = topSelected
    if (sel === null) {
      selectionProvider.selection = new EmptySelection
    } else {
      var props = PropFactory.parseObject(sel, sceneWorker)
      selectionProvider.selection = new PropSelectionList(props, sel.id)
    }
    redraw
  }

  override mouseEnter(MouseEvent e) {}

  override mouseExit(MouseEvent e) {}

  override mouseHover(MouseEvent e) {}

  override dispose() {
    colors.dispose
    fonts.dispose
    cursors.dispose
    super.dispose
  }
}
