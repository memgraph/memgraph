\NeedsTeXFormat{LaTeX2e}
\ProvidesClass{mg-beamer}[2018/03/26 Memgraph Beamer]

\DeclareOption*{\PassOptionsToClass{\CurrentOption}{beamer}}

\ProcessOptions \relax

\LoadClass{beamer}

\usetheme{Pittsburgh}

% Memgraph color palette
\definecolor{mg-purple}{HTML}{720096}
\definecolor{mg-red}{HTML}{DD2222}
\definecolor{mg-orange}{HTML}{FB6E00}
\definecolor{mg-yellow}{HTML}{FFC500}
\definecolor{mg-gray}{HTML}{857F87}
\definecolor{mg-black}{HTML}{231F20}

\RequirePackage{fontspec}
% Title fonts
\setbeamerfont{frametitle}{family={\fontspec[Path = ./mg-style/fonts/]{EncodeSansSemiCondensed-Regular.ttf}}}
\setbeamerfont{title}{family={\fontspec[Path = ./mg-style/fonts/]{EncodeSansSemiCondensed-Regular.ttf}}}
% Body font
\RequirePackage[sfdefault,light]{roboto}
% Roboto is pretty bad for monospace font. We will find a replacement.
% \setmonofont{RobotoMono-Regular.ttf}[Path = ./mg-style/fonts/]

% Title slide styles
% \setbeamerfont{frametitle}{size=\huge}
% \setbeamerfont{title}{size=\huge}
% \setbeamerfont{date}{size=\tiny}

% Other typography styles
\setbeamertemplate{frametitle}[default][center]
\setbeamercolor{frametitle}{fg=mg-black}
\setbeamercolor{title}{fg=mg-black}
\setbeamercolor{section in toc}{fg=mg-black}
\setbeamercolor{local structure}{fg=mg-orange}
\setbeamercolor{alert text}{fg=mg-red}

% Commands
\newcommand{\mgalert}[1]{{\usebeamercolor[fg]{alert text}#1}}
\newcommand{\titleframe}{\frame[plain]{\titlepage}}
\newcommand{\mgtexttt}[1]{{\textcolor{mg-gray}{\texttt{#1}}}}

% Title slide background
\RequirePackage{tikz,calc}
% Use title-slide-169 if aspect ration is 16:9
\pgfdeclareimage[interpolate=true,width=\paperwidth,height=\paperheight]{logo}{mg-style/title-slide-169}
\setbeamertemplate{background}{
  \begin{tikzpicture}
  \useasboundingbox (0,0) rectangle (\the\paperwidth,\the\paperheight);
    \pgftext[at=\pgfpoint{0}{0},left,base]{\pgfuseimage{logo}};
  \ifnum\thepage>1\relax
  \useasboundingbox (0,0) rectangle (\the\paperwidth,\the\paperheight);
      \fill[white, opacity=1](0,\the\paperheight)--(\the\paperwidth,\the\paperheight)--(\the\paperwidth,0)--(0,0)--(0,\the\paperheight);
  \fi
  \end{tikzpicture}
}

% Footline content
\setbeamertemplate{navigation symbols}{}%remove navigation symbols
 \setbeamertemplate{footline}{
  \begin{beamercolorbox}[ht=1.6cm,wd=\paperwidth]{footlinecolor}
    \vspace{0.1cm}
    \hfill
    \begin{minipage}[c]{3cm}
      \begin{center}
       \includegraphics[height=0.8cm]{mg-style/memgraph-logo.png}
      \end{center}
    \end{minipage}
    \begin{minipage}[c]{7cm}
         \insertshorttitle\ --- \insertsection
    \end{minipage}
    \begin{minipage}[c]{2cm}
        \tiny{\insertframenumber{} of \inserttotalframenumber}
    \end{minipage}
   \end{beamercolorbox}
}

\endinput
