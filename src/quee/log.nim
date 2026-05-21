import std/terminal

const QueePrefix* = "[Quee] "

proc queeInfo*(msg: string) =
  styledEcho fgCyan, QueePrefix, resetStyle, msg, "\n"

proc queeWarn*(msg: string) =
  styledEcho fgYellow, QueePrefix, resetStyle, fgYellow, msg, "\n", resetStyle

proc queeError*(msg: string) =
  styledEcho fgRed, QueePrefix, resetStyle, fgRed, msg, "\n", resetStyle

proc queeOk*(msg: string) =
  styledEcho fgGreen, QueePrefix, resetStyle, fgGreen, msg, "\n", resetStyle
