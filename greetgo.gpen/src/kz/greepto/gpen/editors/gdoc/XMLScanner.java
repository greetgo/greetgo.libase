package kz.greepto.gpen.editors.gdoc;

import kz.greepto.gpen.util.ColorManager;

import org.eclipse.jface.text.rules.*;
import org.eclipse.jface.text.*;

public class XMLScanner extends RuleBasedScanner {
  
  public XMLScanner(ColorManager manager) {
    IToken procInstr = new Token(new TextAttribute(manager.fromRGB(IXMLColorConstants.PROC_INSTR)));
    
    IRule[] rules = new IRule[2];
    //Add rule for processing instructions
    rules[0] = new SingleLineRule("<?", "?>", procInstr);
    // Add generic whitespace rule.
    rules[1] = new WhitespaceRule(new XMLWhitespaceDetector());
    
    setRules(rules);
  }
}