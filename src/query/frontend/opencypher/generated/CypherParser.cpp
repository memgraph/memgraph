
// Generated from
// /home/buda/Workspace/code/memgraph/memgraph/src/query/frontend/opencypher/grammar/Cypher.g4
// by ANTLR 4.6

#include "CypherListener.h"
#include "CypherVisitor.h"

#include "CypherParser.h"

using namespace antlrcpp;
using namespace antlropencypher;
using namespace antlr4;

CypherParser::CypherParser(TokenStream *input) : Parser(input) {
  _interpreter = new atn::ParserATNSimulator(this, _atn, _decisionToDFA,
                                             _sharedContextCache);
}

CypherParser::~CypherParser() { delete _interpreter; }

std::string CypherParser::getGrammarFileName() const { return "Cypher.g4"; }

const std::vector<std::string> &CypherParser::getRuleNames() const {
  return _ruleNames;
}

dfa::Vocabulary &CypherParser::getVocabulary() const { return _vocabulary; }

//----------------- CypherContext
//------------------------------------------------------------------

CypherParser::CypherContext::CypherContext(ParserRuleContext *parent,
                                           size_t invokingState)
    : ParserRuleContext(parent, invokingState) {}

CypherParser::StatementContext *CypherParser::CypherContext::statement() {
  return getRuleContext<CypherParser::StatementContext>(0);
}

std::vector<tree::TerminalNode *> CypherParser::CypherContext::SP() {
  return getTokens(CypherParser::SP);
}

tree::TerminalNode *CypherParser::CypherContext::SP(size_t i) {
  return getToken(CypherParser::SP, i);
}

size_t CypherParser::CypherContext::getRuleIndex() const {
  return CypherParser::RuleCypher;
}

void CypherParser::CypherContext::enterRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<CypherListener *>(listener);
  if (parserListener != nullptr) parserListener->enterCypher(this);
}

void CypherParser::CypherContext::exitRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<CypherListener *>(listener);
  if (parserListener != nullptr) parserListener->exitCypher(this);
}

antlrcpp::Any CypherParser::CypherContext::accept(
    tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<CypherVisitor *>(visitor))
    return parserVisitor->visitCypher(this);
  else
    return visitor->visitChildren(this);
}

CypherParser::CypherContext *CypherParser::cypher() {
  CypherContext *_localctx =
      _tracker.createInstance<CypherContext>(_ctx, getState());
  enterRule(_localctx, 0, CypherParser::RuleCypher);
  size_t _la = 0;

  auto onExit = finally([=] { exitRule(); });
  try {
    enterOuterAlt(_localctx, 1);
    setState(159);
    _errHandler->sync(this);

    _la = _input->LA(1);
    if (_la == CypherParser::SP) {
      setState(158);
      match(CypherParser::SP);
    }
    setState(161);
    statement();
    setState(166);
    _errHandler->sync(this);

    switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(
        _input, 2, _ctx)) {
      case 1: {
        setState(163);
        _errHandler->sync(this);

        _la = _input->LA(1);
        if (_la == CypherParser::SP) {
          setState(162);
          match(CypherParser::SP);
        }
        setState(165);
        match(CypherParser::T__0);
        break;
      }
    }
    setState(169);
    _errHandler->sync(this);

    _la = _input->LA(1);
    if (_la == CypherParser::SP) {
      setState(168);
      match(CypherParser::SP);
    }

  } catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- StatementContext
//------------------------------------------------------------------

CypherParser::StatementContext::StatementContext(ParserRuleContext *parent,
                                                 size_t invokingState)
    : ParserRuleContext(parent, invokingState) {}

CypherParser::QueryContext *CypherParser::StatementContext::query() {
  return getRuleContext<CypherParser::QueryContext>(0);
}

size_t CypherParser::StatementContext::getRuleIndex() const {
  return CypherParser::RuleStatement;
}

void CypherParser::StatementContext::enterRule(
    tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<CypherListener *>(listener);
  if (parserListener != nullptr) parserListener->enterStatement(this);
}

void CypherParser::StatementContext::exitRule(
    tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<CypherListener *>(listener);
  if (parserListener != nullptr) parserListener->exitStatement(this);
}

antlrcpp::Any CypherParser::StatementContext::accept(
    tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<CypherVisitor *>(visitor))
    return parserVisitor->visitStatement(this);
  else
    return visitor->visitChildren(this);
}

CypherParser::StatementContext *CypherParser::statement() {
  StatementContext *_localctx =
      _tracker.createInstance<StatementContext>(_ctx, getState());
  enterRule(_localctx, 2, CypherParser::RuleStatement);

  auto onExit = finally([=] { exitRule(); });
  try {
    enterOuterAlt(_localctx, 1);
    setState(171);
    query();

  } catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- QueryContext
//------------------------------------------------------------------

CypherParser::QueryContext::QueryContext(ParserRuleContext *parent,
                                         size_t invokingState)
    : ParserRuleContext(parent, invokingState) {}

CypherParser::RegularQueryContext *CypherParser::QueryContext::regularQuery() {
  return getRuleContext<CypherParser::RegularQueryContext>(0);
}

size_t CypherParser::QueryContext::getRuleIndex() const {
  return CypherParser::RuleQuery;
}

void CypherParser::QueryContext::enterRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<CypherListener *>(listener);
  if (parserListener != nullptr) parserListener->enterQuery(this);
}

void CypherParser::QueryContext::exitRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<CypherListener *>(listener);
  if (parserListener != nullptr) parserListener->exitQuery(this);
}

antlrcpp::Any CypherParser::QueryContext::accept(
    tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<CypherVisitor *>(visitor))
    return parserVisitor->visitQuery(this);
  else
    return visitor->visitChildren(this);
}

CypherParser::QueryContext *CypherParser::query() {
  QueryContext *_localctx =
      _tracker.createInstance<QueryContext>(_ctx, getState());
  enterRule(_localctx, 4, CypherParser::RuleQuery);

  auto onExit = finally([=] { exitRule(); });
  try {
    enterOuterAlt(_localctx, 1);
    setState(173);
    regularQuery();

  } catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- RegularQueryContext
//------------------------------------------------------------------

CypherParser::RegularQueryContext::RegularQueryContext(
    ParserRuleContext *parent, size_t invokingState)
    : ParserRuleContext(parent, invokingState) {}

CypherParser::SingleQueryContext *
CypherParser::RegularQueryContext::singleQuery() {
  return getRuleContext<CypherParser::SingleQueryContext>(0);
}

std::vector<CypherParser::CypherUnionContext *>
CypherParser::RegularQueryContext::cypherUnion() {
  return getRuleContexts<CypherParser::CypherUnionContext>();
}

CypherParser::CypherUnionContext *
CypherParser::RegularQueryContext::cypherUnion(size_t i) {
  return getRuleContext<CypherParser::CypherUnionContext>(i);
}

std::vector<tree::TerminalNode *> CypherParser::RegularQueryContext::SP() {
  return getTokens(CypherParser::SP);
}

tree::TerminalNode *CypherParser::RegularQueryContext::SP(size_t i) {
  return getToken(CypherParser::SP, i);
}

size_t CypherParser::RegularQueryContext::getRuleIndex() const {
  return CypherParser::RuleRegularQuery;
}

void CypherParser::RegularQueryContext::enterRule(
    tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<CypherListener *>(listener);
  if (parserListener != nullptr) parserListener->enterRegularQuery(this);
}

void CypherParser::RegularQueryContext::exitRule(
    tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<CypherListener *>(listener);
  if (parserListener != nullptr) parserListener->exitRegularQuery(this);
}

antlrcpp::Any CypherParser::RegularQueryContext::accept(
    tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<CypherVisitor *>(visitor))
    return parserVisitor->visitRegularQuery(this);
  else
    return visitor->visitChildren(this);
}

CypherParser::RegularQueryContext *CypherParser::regularQuery() {
  RegularQueryContext *_localctx =
      _tracker.createInstance<RegularQueryContext>(_ctx, getState());
  enterRule(_localctx, 6, CypherParser::RuleRegularQuery);
  size_t _la = 0;

  auto onExit = finally([=] { exitRule(); });
  try {
    size_t alt;
    enterOuterAlt(_localctx, 1);
    setState(175);
    singleQuery();
    setState(182);
    _errHandler->sync(this);
    alt = getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 5,
                                                                     _ctx);
    while (alt != 2 && alt != atn::ATN::INVALID_ALT_NUMBER) {
      if (alt == 1) {
        setState(177);
        _errHandler->sync(this);

        _la = _input->LA(1);
        if (_la == CypherParser::SP) {
          setState(176);
          match(CypherParser::SP);
        }
        setState(179);
        cypherUnion();
      }
      setState(184);
      _errHandler->sync(this);
      alt = getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input,
                                                                       5, _ctx);
    }

  } catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- SingleQueryContext
//------------------------------------------------------------------

CypherParser::SingleQueryContext::SingleQueryContext(ParserRuleContext *parent,
                                                     size_t invokingState)
    : ParserRuleContext(parent, invokingState) {}

std::vector<CypherParser::ClauseContext *>
CypherParser::SingleQueryContext::clause() {
  return getRuleContexts<CypherParser::ClauseContext>();
}

CypherParser::ClauseContext *CypherParser::SingleQueryContext::clause(
    size_t i) {
  return getRuleContext<CypherParser::ClauseContext>(i);
}

std::vector<tree::TerminalNode *> CypherParser::SingleQueryContext::SP() {
  return getTokens(CypherParser::SP);
}

tree::TerminalNode *CypherParser::SingleQueryContext::SP(size_t i) {
  return getToken(CypherParser::SP, i);
}

size_t CypherParser::SingleQueryContext::getRuleIndex() const {
  return CypherParser::RuleSingleQuery;
}

void CypherParser::SingleQueryContext::enterRule(
    tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<CypherListener *>(listener);
  if (parserListener != nullptr) parserListener->enterSingleQuery(this);
}

void CypherParser::SingleQueryContext::exitRule(
    tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<CypherListener *>(listener);
  if (parserListener != nullptr) parserListener->exitSingleQuery(this);
}

antlrcpp::Any CypherParser::SingleQueryContext::accept(
    tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<CypherVisitor *>(visitor))
    return parserVisitor->visitSingleQuery(this);
  else
    return visitor->visitChildren(this);
}

CypherParser::SingleQueryContext *CypherParser::singleQuery() {
  SingleQueryContext *_localctx =
      _tracker.createInstance<SingleQueryContext>(_ctx, getState());
  enterRule(_localctx, 8, CypherParser::RuleSingleQuery);
  size_t _la = 0;

  auto onExit = finally([=] { exitRule(); });
  try {
    size_t alt;
    enterOuterAlt(_localctx, 1);
    setState(185);
    clause();
    setState(192);
    _errHandler->sync(this);
    alt = getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 7,
                                                                     _ctx);
    while (alt != 2 && alt != atn::ATN::INVALID_ALT_NUMBER) {
      if (alt == 1) {
        setState(187);
        _errHandler->sync(this);

        _la = _input->LA(1);
        if (_la == CypherParser::SP) {
          setState(186);
          match(CypherParser::SP);
        }
        setState(189);
        clause();
      }
      setState(194);
      _errHandler->sync(this);
      alt = getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input,
                                                                       7, _ctx);
    }

  } catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- CypherUnionContext
//------------------------------------------------------------------

CypherParser::CypherUnionContext::CypherUnionContext(ParserRuleContext *parent,
                                                     size_t invokingState)
    : ParserRuleContext(parent, invokingState) {}

tree::TerminalNode *CypherParser::CypherUnionContext::UNION() {
  return getToken(CypherParser::UNION, 0);
}

std::vector<tree::TerminalNode *> CypherParser::CypherUnionContext::SP() {
  return getTokens(CypherParser::SP);
}

tree::TerminalNode *CypherParser::CypherUnionContext::SP(size_t i) {
  return getToken(CypherParser::SP, i);
}

tree::TerminalNode *CypherParser::CypherUnionContext::ALL() {
  return getToken(CypherParser::ALL, 0);
}

CypherParser::SingleQueryContext *
CypherParser::CypherUnionContext::singleQuery() {
  return getRuleContext<CypherParser::SingleQueryContext>(0);
}

size_t CypherParser::CypherUnionContext::getRuleIndex() const {
  return CypherParser::RuleCypherUnion;
}

void CypherParser::CypherUnionContext::enterRule(
    tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<CypherListener *>(listener);
  if (parserListener != nullptr) parserListener->enterCypherUnion(this);
}

void CypherParser::CypherUnionContext::exitRule(
    tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<CypherListener *>(listener);
  if (parserListener != nullptr) parserListener->exitCypherUnion(this);
}

antlrcpp::Any CypherParser::CypherUnionContext::accept(
    tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<CypherVisitor *>(visitor))
    return parserVisitor->visitCypherUnion(this);
  else
    return visitor->visitChildren(this);
}

CypherParser::CypherUnionContext *CypherParser::cypherUnion() {
  CypherUnionContext *_localctx =
      _tracker.createInstance<CypherUnionContext>(_ctx, getState());
  enterRule(_localctx, 10, CypherParser::RuleCypherUnion);
  size_t _la = 0;

  auto onExit = finally([=] { exitRule(); });
  try {
    setState(207);
    _errHandler->sync(this);
    switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(
        _input, 10, _ctx)) {
      case 1: {
        enterOuterAlt(_localctx, 1);
        setState(195);
        match(CypherParser::UNION);
        setState(196);
        match(CypherParser::SP);
        setState(197);
        match(CypherParser::ALL);
        setState(199);
        _errHandler->sync(this);

        _la = _input->LA(1);
        if (_la == CypherParser::SP) {
          setState(198);
          match(CypherParser::SP);
        }
        setState(201);
        singleQuery();
        break;
      }

      case 2: {
        enterOuterAlt(_localctx, 2);
        setState(202);
        match(CypherParser::UNION);
        setState(204);
        _errHandler->sync(this);

        _la = _input->LA(1);
        if (_la == CypherParser::SP) {
          setState(203);
          match(CypherParser::SP);
        }
        setState(206);
        singleQuery();
        break;
      }
    }

  } catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- ClauseContext
//------------------------------------------------------------------

CypherParser::ClauseContext::ClauseContext(ParserRuleContext *parent,
                                           size_t invokingState)
    : ParserRuleContext(parent, invokingState) {}

CypherParser::CypherMatchContext *CypherParser::ClauseContext::cypherMatch() {
  return getRuleContext<CypherParser::CypherMatchContext>(0);
}

CypherParser::UnwindContext *CypherParser::ClauseContext::unwind() {
  return getRuleContext<CypherParser::UnwindContext>(0);
}

CypherParser::MergeContext *CypherParser::ClauseContext::merge() {
  return getRuleContext<CypherParser::MergeContext>(0);
}

CypherParser::CreateContext *CypherParser::ClauseContext::create() {
  return getRuleContext<CypherParser::CreateContext>(0);
}

CypherParser::SetContext *CypherParser::ClauseContext::set() {
  return getRuleContext<CypherParser::SetContext>(0);
}

CypherParser::CypherDeleteContext *CypherParser::ClauseContext::cypherDelete() {
  return getRuleContext<CypherParser::CypherDeleteContext>(0);
}

CypherParser::RemoveContext *CypherParser::ClauseContext::remove() {
  return getRuleContext<CypherParser::RemoveContext>(0);
}

CypherParser::WithContext *CypherParser::ClauseContext::with() {
  return getRuleContext<CypherParser::WithContext>(0);
}

CypherParser::CypherReturnContext *CypherParser::ClauseContext::cypherReturn() {
  return getRuleContext<CypherParser::CypherReturnContext>(0);
}

size_t CypherParser::ClauseContext::getRuleIndex() const {
  return CypherParser::RuleClause;
}

void CypherParser::ClauseContext::enterRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<CypherListener *>(listener);
  if (parserListener != nullptr) parserListener->enterClause(this);
}

void CypherParser::ClauseContext::exitRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<CypherListener *>(listener);
  if (parserListener != nullptr) parserListener->exitClause(this);
}

antlrcpp::Any CypherParser::ClauseContext::accept(
    tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<CypherVisitor *>(visitor))
    return parserVisitor->visitClause(this);
  else
    return visitor->visitChildren(this);
}

CypherParser::ClauseContext *CypherParser::clause() {
  ClauseContext *_localctx =
      _tracker.createInstance<ClauseContext>(_ctx, getState());
  enterRule(_localctx, 12, CypherParser::RuleClause);

  auto onExit = finally([=] { exitRule(); });
  try {
    setState(218);
    _errHandler->sync(this);
    switch (_input->LA(1)) {
      case CypherParser::OPTIONAL:
      case CypherParser::MATCH: {
        enterOuterAlt(_localctx, 1);
        setState(209);
        cypherMatch();
        break;
      }

      case CypherParser::UNWIND: {
        enterOuterAlt(_localctx, 2);
        setState(210);
        unwind();
        break;
      }

      case CypherParser::MERGE: {
        enterOuterAlt(_localctx, 3);
        setState(211);
        merge();
        break;
      }

      case CypherParser::CREATE: {
        enterOuterAlt(_localctx, 4);
        setState(212);
        create();
        break;
      }

      case CypherParser::SET: {
        enterOuterAlt(_localctx, 5);
        setState(213);
        set();
        break;
      }

      case CypherParser::DETACH:
      case CypherParser::DELETE: {
        enterOuterAlt(_localctx, 6);
        setState(214);
        cypherDelete();
        break;
      }

      case CypherParser::REMOVE: {
        enterOuterAlt(_localctx, 7);
        setState(215);
        remove();
        break;
      }

      case CypherParser::WITH: {
        enterOuterAlt(_localctx, 8);
        setState(216);
        with();
        break;
      }

      case CypherParser::RETURN: {
        enterOuterAlt(_localctx, 9);
        setState(217);
        cypherReturn();
        break;
      }

      default:
        throw NoViableAltException(this);
    }

  } catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- CypherMatchContext
//------------------------------------------------------------------

CypherParser::CypherMatchContext::CypherMatchContext(ParserRuleContext *parent,
                                                     size_t invokingState)
    : ParserRuleContext(parent, invokingState) {}

tree::TerminalNode *CypherParser::CypherMatchContext::MATCH() {
  return getToken(CypherParser::MATCH, 0);
}

CypherParser::PatternContext *CypherParser::CypherMatchContext::pattern() {
  return getRuleContext<CypherParser::PatternContext>(0);
}

tree::TerminalNode *CypherParser::CypherMatchContext::OPTIONAL() {
  return getToken(CypherParser::OPTIONAL, 0);
}

std::vector<tree::TerminalNode *> CypherParser::CypherMatchContext::SP() {
  return getTokens(CypherParser::SP);
}

tree::TerminalNode *CypherParser::CypherMatchContext::SP(size_t i) {
  return getToken(CypherParser::SP, i);
}

CypherParser::WhereContext *CypherParser::CypherMatchContext::where() {
  return getRuleContext<CypherParser::WhereContext>(0);
}

size_t CypherParser::CypherMatchContext::getRuleIndex() const {
  return CypherParser::RuleCypherMatch;
}

void CypherParser::CypherMatchContext::enterRule(
    tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<CypherListener *>(listener);
  if (parserListener != nullptr) parserListener->enterCypherMatch(this);
}

void CypherParser::CypherMatchContext::exitRule(
    tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<CypherListener *>(listener);
  if (parserListener != nullptr) parserListener->exitCypherMatch(this);
}

antlrcpp::Any CypherParser::CypherMatchContext::accept(
    tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<CypherVisitor *>(visitor))
    return parserVisitor->visitCypherMatch(this);
  else
    return visitor->visitChildren(this);
}

CypherParser::CypherMatchContext *CypherParser::cypherMatch() {
  CypherMatchContext *_localctx =
      _tracker.createInstance<CypherMatchContext>(_ctx, getState());
  enterRule(_localctx, 14, CypherParser::RuleCypherMatch);
  size_t _la = 0;

  auto onExit = finally([=] { exitRule(); });
  try {
    enterOuterAlt(_localctx, 1);
    setState(222);
    _errHandler->sync(this);

    _la = _input->LA(1);
    if (_la == CypherParser::OPTIONAL) {
      setState(220);
      match(CypherParser::OPTIONAL);
      setState(221);
      match(CypherParser::SP);
    }
    setState(224);
    match(CypherParser::MATCH);
    setState(226);
    _errHandler->sync(this);

    _la = _input->LA(1);
    if (_la == CypherParser::SP) {
      setState(225);
      match(CypherParser::SP);
    }
    setState(228);
    pattern();
    setState(233);
    _errHandler->sync(this);

    switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(
        _input, 15, _ctx)) {
      case 1: {
        setState(230);
        _errHandler->sync(this);

        _la = _input->LA(1);
        if (_la == CypherParser::SP) {
          setState(229);
          match(CypherParser::SP);
        }
        setState(232);
        where();
        break;
      }
    }

  } catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- UnwindContext
//------------------------------------------------------------------

CypherParser::UnwindContext::UnwindContext(ParserRuleContext *parent,
                                           size_t invokingState)
    : ParserRuleContext(parent, invokingState) {}

tree::TerminalNode *CypherParser::UnwindContext::UNWIND() {
  return getToken(CypherParser::UNWIND, 0);
}

CypherParser::ExpressionContext *CypherParser::UnwindContext::expression() {
  return getRuleContext<CypherParser::ExpressionContext>(0);
}

std::vector<tree::TerminalNode *> CypherParser::UnwindContext::SP() {
  return getTokens(CypherParser::SP);
}

tree::TerminalNode *CypherParser::UnwindContext::SP(size_t i) {
  return getToken(CypherParser::SP, i);
}

tree::TerminalNode *CypherParser::UnwindContext::AS() {
  return getToken(CypherParser::AS, 0);
}

CypherParser::VariableContext *CypherParser::UnwindContext::variable() {
  return getRuleContext<CypherParser::VariableContext>(0);
}

size_t CypherParser::UnwindContext::getRuleIndex() const {
  return CypherParser::RuleUnwind;
}

void CypherParser::UnwindContext::enterRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<CypherListener *>(listener);
  if (parserListener != nullptr) parserListener->enterUnwind(this);
}

void CypherParser::UnwindContext::exitRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<CypherListener *>(listener);
  if (parserListener != nullptr) parserListener->exitUnwind(this);
}

antlrcpp::Any CypherParser::UnwindContext::accept(
    tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<CypherVisitor *>(visitor))
    return parserVisitor->visitUnwind(this);
  else
    return visitor->visitChildren(this);
}

CypherParser::UnwindContext *CypherParser::unwind() {
  UnwindContext *_localctx =
      _tracker.createInstance<UnwindContext>(_ctx, getState());
  enterRule(_localctx, 16, CypherParser::RuleUnwind);
  size_t _la = 0;

  auto onExit = finally([=] { exitRule(); });
  try {
    enterOuterAlt(_localctx, 1);
    setState(235);
    match(CypherParser::UNWIND);
    setState(237);
    _errHandler->sync(this);

    _la = _input->LA(1);
    if (_la == CypherParser::SP) {
      setState(236);
      match(CypherParser::SP);
    }
    setState(239);
    expression();
    setState(240);
    match(CypherParser::SP);
    setState(241);
    match(CypherParser::AS);
    setState(242);
    match(CypherParser::SP);
    setState(243);
    variable();

  } catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- MergeContext
//------------------------------------------------------------------

CypherParser::MergeContext::MergeContext(ParserRuleContext *parent,
                                         size_t invokingState)
    : ParserRuleContext(parent, invokingState) {}

tree::TerminalNode *CypherParser::MergeContext::MERGE() {
  return getToken(CypherParser::MERGE, 0);
}

CypherParser::PatternPartContext *CypherParser::MergeContext::patternPart() {
  return getRuleContext<CypherParser::PatternPartContext>(0);
}

std::vector<tree::TerminalNode *> CypherParser::MergeContext::SP() {
  return getTokens(CypherParser::SP);
}

tree::TerminalNode *CypherParser::MergeContext::SP(size_t i) {
  return getToken(CypherParser::SP, i);
}

std::vector<CypherParser::MergeActionContext *>
CypherParser::MergeContext::mergeAction() {
  return getRuleContexts<CypherParser::MergeActionContext>();
}

CypherParser::MergeActionContext *CypherParser::MergeContext::mergeAction(
    size_t i) {
  return getRuleContext<CypherParser::MergeActionContext>(i);
}

size_t CypherParser::MergeContext::getRuleIndex() const {
  return CypherParser::RuleMerge;
}

void CypherParser::MergeContext::enterRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<CypherListener *>(listener);
  if (parserListener != nullptr) parserListener->enterMerge(this);
}

void CypherParser::MergeContext::exitRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<CypherListener *>(listener);
  if (parserListener != nullptr) parserListener->exitMerge(this);
}

antlrcpp::Any CypherParser::MergeContext::accept(
    tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<CypherVisitor *>(visitor))
    return parserVisitor->visitMerge(this);
  else
    return visitor->visitChildren(this);
}

CypherParser::MergeContext *CypherParser::merge() {
  MergeContext *_localctx =
      _tracker.createInstance<MergeContext>(_ctx, getState());
  enterRule(_localctx, 18, CypherParser::RuleMerge);
  size_t _la = 0;

  auto onExit = finally([=] { exitRule(); });
  try {
    size_t alt;
    enterOuterAlt(_localctx, 1);
    setState(245);
    match(CypherParser::MERGE);
    setState(247);
    _errHandler->sync(this);

    _la = _input->LA(1);
    if (_la == CypherParser::SP) {
      setState(246);
      match(CypherParser::SP);
    }
    setState(249);
    patternPart();
    setState(254);
    _errHandler->sync(this);
    alt = getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 18,
                                                                     _ctx);
    while (alt != 2 && alt != atn::ATN::INVALID_ALT_NUMBER) {
      if (alt == 1) {
        setState(250);
        match(CypherParser::SP);
        setState(251);
        mergeAction();
      }
      setState(256);
      _errHandler->sync(this);
      alt = getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(
          _input, 18, _ctx);
    }

  } catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- MergeActionContext
//------------------------------------------------------------------

CypherParser::MergeActionContext::MergeActionContext(ParserRuleContext *parent,
                                                     size_t invokingState)
    : ParserRuleContext(parent, invokingState) {}

tree::TerminalNode *CypherParser::MergeActionContext::ON() {
  return getToken(CypherParser::ON, 0);
}

std::vector<tree::TerminalNode *> CypherParser::MergeActionContext::SP() {
  return getTokens(CypherParser::SP);
}

tree::TerminalNode *CypherParser::MergeActionContext::SP(size_t i) {
  return getToken(CypherParser::SP, i);
}

tree::TerminalNode *CypherParser::MergeActionContext::MATCH() {
  return getToken(CypherParser::MATCH, 0);
}

CypherParser::SetContext *CypherParser::MergeActionContext::set() {
  return getRuleContext<CypherParser::SetContext>(0);
}

tree::TerminalNode *CypherParser::MergeActionContext::CREATE() {
  return getToken(CypherParser::CREATE, 0);
}

size_t CypherParser::MergeActionContext::getRuleIndex() const {
  return CypherParser::RuleMergeAction;
}

void CypherParser::MergeActionContext::enterRule(
    tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<CypherListener *>(listener);
  if (parserListener != nullptr) parserListener->enterMergeAction(this);
}

void CypherParser::MergeActionContext::exitRule(
    tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<CypherListener *>(listener);
  if (parserListener != nullptr) parserListener->exitMergeAction(this);
}

antlrcpp::Any CypherParser::MergeActionContext::accept(
    tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<CypherVisitor *>(visitor))
    return parserVisitor->visitMergeAction(this);
  else
    return visitor->visitChildren(this);
}

CypherParser::MergeActionContext *CypherParser::mergeAction() {
  MergeActionContext *_localctx =
      _tracker.createInstance<MergeActionContext>(_ctx, getState());
  enterRule(_localctx, 20, CypherParser::RuleMergeAction);

  auto onExit = finally([=] { exitRule(); });
  try {
    setState(267);
    _errHandler->sync(this);
    switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(
        _input, 19, _ctx)) {
      case 1: {
        enterOuterAlt(_localctx, 1);
        setState(257);
        match(CypherParser::ON);
        setState(258);
        match(CypherParser::SP);
        setState(259);
        match(CypherParser::MATCH);
        setState(260);
        match(CypherParser::SP);
        setState(261);
        set();
        break;
      }

      case 2: {
        enterOuterAlt(_localctx, 2);
        setState(262);
        match(CypherParser::ON);
        setState(263);
        match(CypherParser::SP);
        setState(264);
        match(CypherParser::CREATE);
        setState(265);
        match(CypherParser::SP);
        setState(266);
        set();
        break;
      }
    }

  } catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- CreateContext
//------------------------------------------------------------------

CypherParser::CreateContext::CreateContext(ParserRuleContext *parent,
                                           size_t invokingState)
    : ParserRuleContext(parent, invokingState) {}

tree::TerminalNode *CypherParser::CreateContext::CREATE() {
  return getToken(CypherParser::CREATE, 0);
}

CypherParser::PatternContext *CypherParser::CreateContext::pattern() {
  return getRuleContext<CypherParser::PatternContext>(0);
}

tree::TerminalNode *CypherParser::CreateContext::SP() {
  return getToken(CypherParser::SP, 0);
}

size_t CypherParser::CreateContext::getRuleIndex() const {
  return CypherParser::RuleCreate;
}

void CypherParser::CreateContext::enterRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<CypherListener *>(listener);
  if (parserListener != nullptr) parserListener->enterCreate(this);
}

void CypherParser::CreateContext::exitRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<CypherListener *>(listener);
  if (parserListener != nullptr) parserListener->exitCreate(this);
}

antlrcpp::Any CypherParser::CreateContext::accept(
    tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<CypherVisitor *>(visitor))
    return parserVisitor->visitCreate(this);
  else
    return visitor->visitChildren(this);
}

CypherParser::CreateContext *CypherParser::create() {
  CreateContext *_localctx =
      _tracker.createInstance<CreateContext>(_ctx, getState());
  enterRule(_localctx, 22, CypherParser::RuleCreate);
  size_t _la = 0;

  auto onExit = finally([=] { exitRule(); });
  try {
    enterOuterAlt(_localctx, 1);
    setState(269);
    match(CypherParser::CREATE);
    setState(271);
    _errHandler->sync(this);

    _la = _input->LA(1);
    if (_la == CypherParser::SP) {
      setState(270);
      match(CypherParser::SP);
    }
    setState(273);
    pattern();

  } catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- SetContext
//------------------------------------------------------------------

CypherParser::SetContext::SetContext(ParserRuleContext *parent,
                                     size_t invokingState)
    : ParserRuleContext(parent, invokingState) {}

tree::TerminalNode *CypherParser::SetContext::SET() {
  return getToken(CypherParser::SET, 0);
}

std::vector<CypherParser::SetItemContext *>
CypherParser::SetContext::setItem() {
  return getRuleContexts<CypherParser::SetItemContext>();
}

CypherParser::SetItemContext *CypherParser::SetContext::setItem(size_t i) {
  return getRuleContext<CypherParser::SetItemContext>(i);
}

size_t CypherParser::SetContext::getRuleIndex() const {
  return CypherParser::RuleSet;
}

void CypherParser::SetContext::enterRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<CypherListener *>(listener);
  if (parserListener != nullptr) parserListener->enterSet(this);
}

void CypherParser::SetContext::exitRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<CypherListener *>(listener);
  if (parserListener != nullptr) parserListener->exitSet(this);
}

antlrcpp::Any CypherParser::SetContext::accept(
    tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<CypherVisitor *>(visitor))
    return parserVisitor->visitSet(this);
  else
    return visitor->visitChildren(this);
}

CypherParser::SetContext *CypherParser::set() {
  SetContext *_localctx = _tracker.createInstance<SetContext>(_ctx, getState());
  enterRule(_localctx, 24, CypherParser::RuleSet);
  size_t _la = 0;

  auto onExit = finally([=] { exitRule(); });
  try {
    enterOuterAlt(_localctx, 1);
    setState(275);
    match(CypherParser::SET);
    setState(276);
    setItem();
    setState(281);
    _errHandler->sync(this);
    _la = _input->LA(1);
    while (_la == CypherParser::T__1) {
      setState(277);
      match(CypherParser::T__1);
      setState(278);
      setItem();
      setState(283);
      _errHandler->sync(this);
      _la = _input->LA(1);
    }

  } catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- SetItemContext
//------------------------------------------------------------------

CypherParser::SetItemContext::SetItemContext(ParserRuleContext *parent,
                                             size_t invokingState)
    : ParserRuleContext(parent, invokingState) {}

CypherParser::PropertyExpressionContext *
CypherParser::SetItemContext::propertyExpression() {
  return getRuleContext<CypherParser::PropertyExpressionContext>(0);
}

CypherParser::ExpressionContext *CypherParser::SetItemContext::expression() {
  return getRuleContext<CypherParser::ExpressionContext>(0);
}

CypherParser::VariableContext *CypherParser::SetItemContext::variable() {
  return getRuleContext<CypherParser::VariableContext>(0);
}

CypherParser::NodeLabelsContext *CypherParser::SetItemContext::nodeLabels() {
  return getRuleContext<CypherParser::NodeLabelsContext>(0);
}

size_t CypherParser::SetItemContext::getRuleIndex() const {
  return CypherParser::RuleSetItem;
}

void CypherParser::SetItemContext::enterRule(
    tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<CypherListener *>(listener);
  if (parserListener != nullptr) parserListener->enterSetItem(this);
}

void CypherParser::SetItemContext::exitRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<CypherListener *>(listener);
  if (parserListener != nullptr) parserListener->exitSetItem(this);
}

antlrcpp::Any CypherParser::SetItemContext::accept(
    tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<CypherVisitor *>(visitor))
    return parserVisitor->visitSetItem(this);
  else
    return visitor->visitChildren(this);
}

CypherParser::SetItemContext *CypherParser::setItem() {
  SetItemContext *_localctx =
      _tracker.createInstance<SetItemContext>(_ctx, getState());
  enterRule(_localctx, 26, CypherParser::RuleSetItem);

  auto onExit = finally([=] { exitRule(); });
  try {
    setState(299);
    _errHandler->sync(this);
    switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(
        _input, 22, _ctx)) {
      case 1: {
        enterOuterAlt(_localctx, 1);
        setState(284);
        propertyExpression();
        setState(285);
        match(CypherParser::T__2);
        setState(286);
        expression();
        break;
      }

      case 2: {
        enterOuterAlt(_localctx, 2);
        setState(288);
        variable();
        setState(289);
        match(CypherParser::T__2);
        setState(290);
        expression();
        break;
      }

      case 3: {
        enterOuterAlt(_localctx, 3);
        setState(292);
        variable();
        setState(293);
        match(CypherParser::T__3);
        setState(294);
        expression();
        break;
      }

      case 4: {
        enterOuterAlt(_localctx, 4);
        setState(296);
        variable();
        setState(297);
        nodeLabels();
        break;
      }
    }

  } catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- CypherDeleteContext
//------------------------------------------------------------------

CypherParser::CypherDeleteContext::CypherDeleteContext(
    ParserRuleContext *parent, size_t invokingState)
    : ParserRuleContext(parent, invokingState) {}

tree::TerminalNode *CypherParser::CypherDeleteContext::DELETE() {
  return getToken(CypherParser::DELETE, 0);
}

std::vector<CypherParser::ExpressionContext *>
CypherParser::CypherDeleteContext::expression() {
  return getRuleContexts<CypherParser::ExpressionContext>();
}

CypherParser::ExpressionContext *CypherParser::CypherDeleteContext::expression(
    size_t i) {
  return getRuleContext<CypherParser::ExpressionContext>(i);
}

tree::TerminalNode *CypherParser::CypherDeleteContext::DETACH() {
  return getToken(CypherParser::DETACH, 0);
}

std::vector<tree::TerminalNode *> CypherParser::CypherDeleteContext::SP() {
  return getTokens(CypherParser::SP);
}

tree::TerminalNode *CypherParser::CypherDeleteContext::SP(size_t i) {
  return getToken(CypherParser::SP, i);
}

size_t CypherParser::CypherDeleteContext::getRuleIndex() const {
  return CypherParser::RuleCypherDelete;
}

void CypherParser::CypherDeleteContext::enterRule(
    tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<CypherListener *>(listener);
  if (parserListener != nullptr) parserListener->enterCypherDelete(this);
}

void CypherParser::CypherDeleteContext::exitRule(
    tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<CypherListener *>(listener);
  if (parserListener != nullptr) parserListener->exitCypherDelete(this);
}

antlrcpp::Any CypherParser::CypherDeleteContext::accept(
    tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<CypherVisitor *>(visitor))
    return parserVisitor->visitCypherDelete(this);
  else
    return visitor->visitChildren(this);
}

CypherParser::CypherDeleteContext *CypherParser::cypherDelete() {
  CypherDeleteContext *_localctx =
      _tracker.createInstance<CypherDeleteContext>(_ctx, getState());
  enterRule(_localctx, 28, CypherParser::RuleCypherDelete);
  size_t _la = 0;

  auto onExit = finally([=] { exitRule(); });
  try {
    size_t alt;
    enterOuterAlt(_localctx, 1);
    setState(303);
    _errHandler->sync(this);

    _la = _input->LA(1);
    if (_la == CypherParser::DETACH) {
      setState(301);
      match(CypherParser::DETACH);
      setState(302);
      match(CypherParser::SP);
    }
    setState(305);
    match(CypherParser::DELETE);
    setState(307);
    _errHandler->sync(this);

    _la = _input->LA(1);
    if (_la == CypherParser::SP) {
      setState(306);
      match(CypherParser::SP);
    }
    setState(309);
    expression();
    setState(320);
    _errHandler->sync(this);
    alt = getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 27,
                                                                     _ctx);
    while (alt != 2 && alt != atn::ATN::INVALID_ALT_NUMBER) {
      if (alt == 1) {
        setState(311);
        _errHandler->sync(this);

        _la = _input->LA(1);
        if (_la == CypherParser::SP) {
          setState(310);
          match(CypherParser::SP);
        }
        setState(313);
        match(CypherParser::T__1);
        setState(315);
        _errHandler->sync(this);

        _la = _input->LA(1);
        if (_la == CypherParser::SP) {
          setState(314);
          match(CypherParser::SP);
        }
        setState(317);
        expression();
      }
      setState(322);
      _errHandler->sync(this);
      alt = getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(
          _input, 27, _ctx);
    }

  } catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- RemoveContext
//------------------------------------------------------------------

CypherParser::RemoveContext::RemoveContext(ParserRuleContext *parent,
                                           size_t invokingState)
    : ParserRuleContext(parent, invokingState) {}

tree::TerminalNode *CypherParser::RemoveContext::REMOVE() {
  return getToken(CypherParser::REMOVE, 0);
}

std::vector<tree::TerminalNode *> CypherParser::RemoveContext::SP() {
  return getTokens(CypherParser::SP);
}

tree::TerminalNode *CypherParser::RemoveContext::SP(size_t i) {
  return getToken(CypherParser::SP, i);
}

std::vector<CypherParser::RemoveItemContext *>
CypherParser::RemoveContext::removeItem() {
  return getRuleContexts<CypherParser::RemoveItemContext>();
}

CypherParser::RemoveItemContext *CypherParser::RemoveContext::removeItem(
    size_t i) {
  return getRuleContext<CypherParser::RemoveItemContext>(i);
}

size_t CypherParser::RemoveContext::getRuleIndex() const {
  return CypherParser::RuleRemove;
}

void CypherParser::RemoveContext::enterRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<CypherListener *>(listener);
  if (parserListener != nullptr) parserListener->enterRemove(this);
}

void CypherParser::RemoveContext::exitRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<CypherListener *>(listener);
  if (parserListener != nullptr) parserListener->exitRemove(this);
}

antlrcpp::Any CypherParser::RemoveContext::accept(
    tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<CypherVisitor *>(visitor))
    return parserVisitor->visitRemove(this);
  else
    return visitor->visitChildren(this);
}

CypherParser::RemoveContext *CypherParser::remove() {
  RemoveContext *_localctx =
      _tracker.createInstance<RemoveContext>(_ctx, getState());
  enterRule(_localctx, 30, CypherParser::RuleRemove);
  size_t _la = 0;

  auto onExit = finally([=] { exitRule(); });
  try {
    size_t alt;
    enterOuterAlt(_localctx, 1);
    setState(323);
    match(CypherParser::REMOVE);
    setState(324);
    match(CypherParser::SP);
    setState(325);
    removeItem();
    setState(336);
    _errHandler->sync(this);
    alt = getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 30,
                                                                     _ctx);
    while (alt != 2 && alt != atn::ATN::INVALID_ALT_NUMBER) {
      if (alt == 1) {
        setState(327);
        _errHandler->sync(this);

        _la = _input->LA(1);
        if (_la == CypherParser::SP) {
          setState(326);
          match(CypherParser::SP);
        }
        setState(329);
        match(CypherParser::T__1);
        setState(331);
        _errHandler->sync(this);

        _la = _input->LA(1);
        if (_la == CypherParser::SP) {
          setState(330);
          match(CypherParser::SP);
        }
        setState(333);
        removeItem();
      }
      setState(338);
      _errHandler->sync(this);
      alt = getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(
          _input, 30, _ctx);
    }

  } catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- RemoveItemContext
//------------------------------------------------------------------

CypherParser::RemoveItemContext::RemoveItemContext(ParserRuleContext *parent,
                                                   size_t invokingState)
    : ParserRuleContext(parent, invokingState) {}

CypherParser::VariableContext *CypherParser::RemoveItemContext::variable() {
  return getRuleContext<CypherParser::VariableContext>(0);
}

CypherParser::NodeLabelsContext *CypherParser::RemoveItemContext::nodeLabels() {
  return getRuleContext<CypherParser::NodeLabelsContext>(0);
}

CypherParser::PropertyExpressionContext *
CypherParser::RemoveItemContext::propertyExpression() {
  return getRuleContext<CypherParser::PropertyExpressionContext>(0);
}

size_t CypherParser::RemoveItemContext::getRuleIndex() const {
  return CypherParser::RuleRemoveItem;
}

void CypherParser::RemoveItemContext::enterRule(
    tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<CypherListener *>(listener);
  if (parserListener != nullptr) parserListener->enterRemoveItem(this);
}

void CypherParser::RemoveItemContext::exitRule(
    tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<CypherListener *>(listener);
  if (parserListener != nullptr) parserListener->exitRemoveItem(this);
}

antlrcpp::Any CypherParser::RemoveItemContext::accept(
    tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<CypherVisitor *>(visitor))
    return parserVisitor->visitRemoveItem(this);
  else
    return visitor->visitChildren(this);
}

CypherParser::RemoveItemContext *CypherParser::removeItem() {
  RemoveItemContext *_localctx =
      _tracker.createInstance<RemoveItemContext>(_ctx, getState());
  enterRule(_localctx, 32, CypherParser::RuleRemoveItem);

  auto onExit = finally([=] { exitRule(); });
  try {
    setState(343);
    _errHandler->sync(this);
    switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(
        _input, 31, _ctx)) {
      case 1: {
        enterOuterAlt(_localctx, 1);
        setState(339);
        variable();
        setState(340);
        nodeLabels();
        break;
      }

      case 2: {
        enterOuterAlt(_localctx, 2);
        setState(342);
        propertyExpression();
        break;
      }
    }

  } catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- WithContext
//------------------------------------------------------------------

CypherParser::WithContext::WithContext(ParserRuleContext *parent,
                                       size_t invokingState)
    : ParserRuleContext(parent, invokingState) {}

tree::TerminalNode *CypherParser::WithContext::WITH() {
  return getToken(CypherParser::WITH, 0);
}

std::vector<tree::TerminalNode *> CypherParser::WithContext::SP() {
  return getTokens(CypherParser::SP);
}

tree::TerminalNode *CypherParser::WithContext::SP(size_t i) {
  return getToken(CypherParser::SP, i);
}

CypherParser::ReturnBodyContext *CypherParser::WithContext::returnBody() {
  return getRuleContext<CypherParser::ReturnBodyContext>(0);
}

tree::TerminalNode *CypherParser::WithContext::DISTINCT() {
  return getToken(CypherParser::DISTINCT, 0);
}

CypherParser::WhereContext *CypherParser::WithContext::where() {
  return getRuleContext<CypherParser::WhereContext>(0);
}

size_t CypherParser::WithContext::getRuleIndex() const {
  return CypherParser::RuleWith;
}

void CypherParser::WithContext::enterRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<CypherListener *>(listener);
  if (parserListener != nullptr) parserListener->enterWith(this);
}

void CypherParser::WithContext::exitRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<CypherListener *>(listener);
  if (parserListener != nullptr) parserListener->exitWith(this);
}

antlrcpp::Any CypherParser::WithContext::accept(
    tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<CypherVisitor *>(visitor))
    return parserVisitor->visitWith(this);
  else
    return visitor->visitChildren(this);
}

CypherParser::WithContext *CypherParser::with() {
  WithContext *_localctx =
      _tracker.createInstance<WithContext>(_ctx, getState());
  enterRule(_localctx, 34, CypherParser::RuleWith);
  size_t _la = 0;

  auto onExit = finally([=] { exitRule(); });
  try {
    enterOuterAlt(_localctx, 1);
    setState(345);
    match(CypherParser::WITH);
    setState(350);
    _errHandler->sync(this);

    switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(
        _input, 33, _ctx)) {
      case 1: {
        setState(347);
        _errHandler->sync(this);

        _la = _input->LA(1);
        if (_la == CypherParser::SP) {
          setState(346);
          match(CypherParser::SP);
        }
        setState(349);
        match(CypherParser::DISTINCT);
        break;
      }
    }
    setState(352);
    match(CypherParser::SP);
    setState(353);
    returnBody();
    setState(358);
    _errHandler->sync(this);

    switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(
        _input, 35, _ctx)) {
      case 1: {
        setState(355);
        _errHandler->sync(this);

        _la = _input->LA(1);
        if (_la == CypherParser::SP) {
          setState(354);
          match(CypherParser::SP);
        }
        setState(357);
        where();
        break;
      }
    }

  } catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- CypherReturnContext
//------------------------------------------------------------------

CypherParser::CypherReturnContext::CypherReturnContext(
    ParserRuleContext *parent, size_t invokingState)
    : ParserRuleContext(parent, invokingState) {}

tree::TerminalNode *CypherParser::CypherReturnContext::RETURN() {
  return getToken(CypherParser::RETURN, 0);
}

std::vector<tree::TerminalNode *> CypherParser::CypherReturnContext::SP() {
  return getTokens(CypherParser::SP);
}

tree::TerminalNode *CypherParser::CypherReturnContext::SP(size_t i) {
  return getToken(CypherParser::SP, i);
}

CypherParser::ReturnBodyContext *
CypherParser::CypherReturnContext::returnBody() {
  return getRuleContext<CypherParser::ReturnBodyContext>(0);
}

tree::TerminalNode *CypherParser::CypherReturnContext::DISTINCT() {
  return getToken(CypherParser::DISTINCT, 0);
}

size_t CypherParser::CypherReturnContext::getRuleIndex() const {
  return CypherParser::RuleCypherReturn;
}

void CypherParser::CypherReturnContext::enterRule(
    tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<CypherListener *>(listener);
  if (parserListener != nullptr) parserListener->enterCypherReturn(this);
}

void CypherParser::CypherReturnContext::exitRule(
    tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<CypherListener *>(listener);
  if (parserListener != nullptr) parserListener->exitCypherReturn(this);
}

antlrcpp::Any CypherParser::CypherReturnContext::accept(
    tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<CypherVisitor *>(visitor))
    return parserVisitor->visitCypherReturn(this);
  else
    return visitor->visitChildren(this);
}

CypherParser::CypherReturnContext *CypherParser::cypherReturn() {
  CypherReturnContext *_localctx =
      _tracker.createInstance<CypherReturnContext>(_ctx, getState());
  enterRule(_localctx, 36, CypherParser::RuleCypherReturn);
  size_t _la = 0;

  auto onExit = finally([=] { exitRule(); });
  try {
    enterOuterAlt(_localctx, 1);
    setState(360);
    match(CypherParser::RETURN);
    setState(365);
    _errHandler->sync(this);

    switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(
        _input, 37, _ctx)) {
      case 1: {
        setState(362);
        _errHandler->sync(this);

        _la = _input->LA(1);
        if (_la == CypherParser::SP) {
          setState(361);
          match(CypherParser::SP);
        }
        setState(364);
        match(CypherParser::DISTINCT);
        break;
      }
    }
    setState(367);
    match(CypherParser::SP);
    setState(368);
    returnBody();

  } catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- ReturnBodyContext
//------------------------------------------------------------------

CypherParser::ReturnBodyContext::ReturnBodyContext(ParserRuleContext *parent,
                                                   size_t invokingState)
    : ParserRuleContext(parent, invokingState) {}

CypherParser::ReturnItemsContext *
CypherParser::ReturnBodyContext::returnItems() {
  return getRuleContext<CypherParser::ReturnItemsContext>(0);
}

std::vector<tree::TerminalNode *> CypherParser::ReturnBodyContext::SP() {
  return getTokens(CypherParser::SP);
}

tree::TerminalNode *CypherParser::ReturnBodyContext::SP(size_t i) {
  return getToken(CypherParser::SP, i);
}

CypherParser::OrderContext *CypherParser::ReturnBodyContext::order() {
  return getRuleContext<CypherParser::OrderContext>(0);
}

CypherParser::SkipContext *CypherParser::ReturnBodyContext::skip() {
  return getRuleContext<CypherParser::SkipContext>(0);
}

CypherParser::LimitContext *CypherParser::ReturnBodyContext::limit() {
  return getRuleContext<CypherParser::LimitContext>(0);
}

size_t CypherParser::ReturnBodyContext::getRuleIndex() const {
  return CypherParser::RuleReturnBody;
}

void CypherParser::ReturnBodyContext::enterRule(
    tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<CypherListener *>(listener);
  if (parserListener != nullptr) parserListener->enterReturnBody(this);
}

void CypherParser::ReturnBodyContext::exitRule(
    tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<CypherListener *>(listener);
  if (parserListener != nullptr) parserListener->exitReturnBody(this);
}

antlrcpp::Any CypherParser::ReturnBodyContext::accept(
    tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<CypherVisitor *>(visitor))
    return parserVisitor->visitReturnBody(this);
  else
    return visitor->visitChildren(this);
}

CypherParser::ReturnBodyContext *CypherParser::returnBody() {
  ReturnBodyContext *_localctx =
      _tracker.createInstance<ReturnBodyContext>(_ctx, getState());
  enterRule(_localctx, 38, CypherParser::RuleReturnBody);

  auto onExit = finally([=] { exitRule(); });
  try {
    enterOuterAlt(_localctx, 1);
    setState(370);
    returnItems();
    setState(373);
    _errHandler->sync(this);

    switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(
        _input, 38, _ctx)) {
      case 1: {
        setState(371);
        match(CypherParser::SP);
        setState(372);
        order();
        break;
      }
    }
    setState(377);
    _errHandler->sync(this);

    switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(
        _input, 39, _ctx)) {
      case 1: {
        setState(375);
        match(CypherParser::SP);
        setState(376);
        skip();
        break;
      }
    }
    setState(381);
    _errHandler->sync(this);

    switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(
        _input, 40, _ctx)) {
      case 1: {
        setState(379);
        match(CypherParser::SP);
        setState(380);
        limit();
        break;
      }
    }

  } catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- ReturnItemsContext
//------------------------------------------------------------------

CypherParser::ReturnItemsContext::ReturnItemsContext(ParserRuleContext *parent,
                                                     size_t invokingState)
    : ParserRuleContext(parent, invokingState) {}

std::vector<CypherParser::ReturnItemContext *>
CypherParser::ReturnItemsContext::returnItem() {
  return getRuleContexts<CypherParser::ReturnItemContext>();
}

CypherParser::ReturnItemContext *CypherParser::ReturnItemsContext::returnItem(
    size_t i) {
  return getRuleContext<CypherParser::ReturnItemContext>(i);
}

std::vector<tree::TerminalNode *> CypherParser::ReturnItemsContext::SP() {
  return getTokens(CypherParser::SP);
}

tree::TerminalNode *CypherParser::ReturnItemsContext::SP(size_t i) {
  return getToken(CypherParser::SP, i);
}

size_t CypherParser::ReturnItemsContext::getRuleIndex() const {
  return CypherParser::RuleReturnItems;
}

void CypherParser::ReturnItemsContext::enterRule(
    tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<CypherListener *>(listener);
  if (parserListener != nullptr) parserListener->enterReturnItems(this);
}

void CypherParser::ReturnItemsContext::exitRule(
    tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<CypherListener *>(listener);
  if (parserListener != nullptr) parserListener->exitReturnItems(this);
}

antlrcpp::Any CypherParser::ReturnItemsContext::accept(
    tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<CypherVisitor *>(visitor))
    return parserVisitor->visitReturnItems(this);
  else
    return visitor->visitChildren(this);
}

CypherParser::ReturnItemsContext *CypherParser::returnItems() {
  ReturnItemsContext *_localctx =
      _tracker.createInstance<ReturnItemsContext>(_ctx, getState());
  enterRule(_localctx, 40, CypherParser::RuleReturnItems);
  size_t _la = 0;

  auto onExit = finally([=] { exitRule(); });
  try {
    size_t alt;
    setState(411);
    _errHandler->sync(this);
    switch (_input->LA(1)) {
      case CypherParser::T__4: {
        enterOuterAlt(_localctx, 1);
        setState(383);
        match(CypherParser::T__4);
        setState(394);
        _errHandler->sync(this);
        alt = getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(
            _input, 43, _ctx);
        while (alt != 2 && alt != atn::ATN::INVALID_ALT_NUMBER) {
          if (alt == 1) {
            setState(385);
            _errHandler->sync(this);

            _la = _input->LA(1);
            if (_la == CypherParser::SP) {
              setState(384);
              match(CypherParser::SP);
            }
            setState(387);
            match(CypherParser::T__1);
            setState(389);
            _errHandler->sync(this);

            _la = _input->LA(1);
            if (_la == CypherParser::SP) {
              setState(388);
              match(CypherParser::SP);
            }
            setState(391);
            returnItem();
          }
          setState(396);
          _errHandler->sync(this);
          alt = getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(
              _input, 43, _ctx);
        }
        break;
      }

      case CypherParser::T__5:
      case CypherParser::T__7:
      case CypherParser::T__13:
      case CypherParser::T__14:
      case CypherParser::T__27:
      case CypherParser::T__29:
      case CypherParser::StringLiteral:
      case CypherParser::HexInteger:
      case CypherParser::DecimalInteger:
      case CypherParser::OctalInteger:
      case CypherParser::HexLetter:
      case CypherParser::ExponentDecimalReal:
      case CypherParser::RegularDecimalReal:
      case CypherParser::UNION:
      case CypherParser::ALL:
      case CypherParser::OPTIONAL:
      case CypherParser::MATCH:
      case CypherParser::UNWIND:
      case CypherParser::AS:
      case CypherParser::MERGE:
      case CypherParser::ON:
      case CypherParser::CREATE:
      case CypherParser::SET:
      case CypherParser::DETACH:
      case CypherParser::DELETE:
      case CypherParser::REMOVE:
      case CypherParser::WITH:
      case CypherParser::DISTINCT:
      case CypherParser::RETURN:
      case CypherParser::ORDER:
      case CypherParser::BY:
      case CypherParser::L_SKIP:
      case CypherParser::LIMIT:
      case CypherParser::ASCENDING:
      case CypherParser::ASC:
      case CypherParser::DESCENDING:
      case CypherParser::DESC:
      case CypherParser::WHERE:
      case CypherParser::OR:
      case CypherParser::XOR:
      case CypherParser::AND:
      case CypherParser::NOT:
      case CypherParser::IN:
      case CypherParser::STARTS:
      case CypherParser::ENDS:
      case CypherParser::CONTAINS:
      case CypherParser::IS:
      case CypherParser::CYPHERNULL:
      case CypherParser::COUNT:
      case CypherParser::FILTER:
      case CypherParser::EXTRACT:
      case CypherParser::ANY:
      case CypherParser::NONE:
      case CypherParser::SINGLE:
      case CypherParser::TRUE:
      case CypherParser::FALSE:
      case CypherParser::UnescapedSymbolicName:
      case CypherParser::EscapedSymbolicName: {
        enterOuterAlt(_localctx, 2);
        setState(397);
        returnItem();
        setState(408);
        _errHandler->sync(this);
        alt = getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(
            _input, 46, _ctx);
        while (alt != 2 && alt != atn::ATN::INVALID_ALT_NUMBER) {
          if (alt == 1) {
            setState(399);
            _errHandler->sync(this);

            _la = _input->LA(1);
            if (_la == CypherParser::SP) {
              setState(398);
              match(CypherParser::SP);
            }
            setState(401);
            match(CypherParser::T__1);
            setState(403);
            _errHandler->sync(this);

            _la = _input->LA(1);
            if (_la == CypherParser::SP) {
              setState(402);
              match(CypherParser::SP);
            }
            setState(405);
            returnItem();
          }
          setState(410);
          _errHandler->sync(this);
          alt = getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(
              _input, 46, _ctx);
        }
        break;
      }

      default:
        throw NoViableAltException(this);
    }

  } catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- ReturnItemContext
//------------------------------------------------------------------

CypherParser::ReturnItemContext::ReturnItemContext(ParserRuleContext *parent,
                                                   size_t invokingState)
    : ParserRuleContext(parent, invokingState) {}

CypherParser::ExpressionContext *CypherParser::ReturnItemContext::expression() {
  return getRuleContext<CypherParser::ExpressionContext>(0);
}

std::vector<tree::TerminalNode *> CypherParser::ReturnItemContext::SP() {
  return getTokens(CypherParser::SP);
}

tree::TerminalNode *CypherParser::ReturnItemContext::SP(size_t i) {
  return getToken(CypherParser::SP, i);
}

tree::TerminalNode *CypherParser::ReturnItemContext::AS() {
  return getToken(CypherParser::AS, 0);
}

CypherParser::VariableContext *CypherParser::ReturnItemContext::variable() {
  return getRuleContext<CypherParser::VariableContext>(0);
}

size_t CypherParser::ReturnItemContext::getRuleIndex() const {
  return CypherParser::RuleReturnItem;
}

void CypherParser::ReturnItemContext::enterRule(
    tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<CypherListener *>(listener);
  if (parserListener != nullptr) parserListener->enterReturnItem(this);
}

void CypherParser::ReturnItemContext::exitRule(
    tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<CypherListener *>(listener);
  if (parserListener != nullptr) parserListener->exitReturnItem(this);
}

antlrcpp::Any CypherParser::ReturnItemContext::accept(
    tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<CypherVisitor *>(visitor))
    return parserVisitor->visitReturnItem(this);
  else
    return visitor->visitChildren(this);
}

CypherParser::ReturnItemContext *CypherParser::returnItem() {
  ReturnItemContext *_localctx =
      _tracker.createInstance<ReturnItemContext>(_ctx, getState());
  enterRule(_localctx, 42, CypherParser::RuleReturnItem);

  auto onExit = finally([=] { exitRule(); });
  try {
    setState(420);
    _errHandler->sync(this);
    switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(
        _input, 48, _ctx)) {
      case 1: {
        enterOuterAlt(_localctx, 1);
        setState(413);
        expression();
        setState(414);
        match(CypherParser::SP);
        setState(415);
        match(CypherParser::AS);
        setState(416);
        match(CypherParser::SP);
        setState(417);
        variable();
        break;
      }

      case 2: {
        enterOuterAlt(_localctx, 2);
        setState(419);
        expression();
        break;
      }
    }

  } catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- OrderContext
//------------------------------------------------------------------

CypherParser::OrderContext::OrderContext(ParserRuleContext *parent,
                                         size_t invokingState)
    : ParserRuleContext(parent, invokingState) {}

tree::TerminalNode *CypherParser::OrderContext::ORDER() {
  return getToken(CypherParser::ORDER, 0);
}

std::vector<tree::TerminalNode *> CypherParser::OrderContext::SP() {
  return getTokens(CypherParser::SP);
}

tree::TerminalNode *CypherParser::OrderContext::SP(size_t i) {
  return getToken(CypherParser::SP, i);
}

tree::TerminalNode *CypherParser::OrderContext::BY() {
  return getToken(CypherParser::BY, 0);
}

std::vector<CypherParser::SortItemContext *>
CypherParser::OrderContext::sortItem() {
  return getRuleContexts<CypherParser::SortItemContext>();
}

CypherParser::SortItemContext *CypherParser::OrderContext::sortItem(size_t i) {
  return getRuleContext<CypherParser::SortItemContext>(i);
}

size_t CypherParser::OrderContext::getRuleIndex() const {
  return CypherParser::RuleOrder;
}

void CypherParser::OrderContext::enterRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<CypherListener *>(listener);
  if (parserListener != nullptr) parserListener->enterOrder(this);
}

void CypherParser::OrderContext::exitRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<CypherListener *>(listener);
  if (parserListener != nullptr) parserListener->exitOrder(this);
}

antlrcpp::Any CypherParser::OrderContext::accept(
    tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<CypherVisitor *>(visitor))
    return parserVisitor->visitOrder(this);
  else
    return visitor->visitChildren(this);
}

CypherParser::OrderContext *CypherParser::order() {
  OrderContext *_localctx =
      _tracker.createInstance<OrderContext>(_ctx, getState());
  enterRule(_localctx, 44, CypherParser::RuleOrder);
  size_t _la = 0;

  auto onExit = finally([=] { exitRule(); });
  try {
    enterOuterAlt(_localctx, 1);
    setState(422);
    match(CypherParser::ORDER);
    setState(423);
    match(CypherParser::SP);
    setState(424);
    match(CypherParser::BY);
    setState(425);
    match(CypherParser::SP);
    setState(426);
    sortItem();
    setState(434);
    _errHandler->sync(this);
    _la = _input->LA(1);
    while (_la == CypherParser::T__1) {
      setState(427);
      match(CypherParser::T__1);
      setState(429);
      _errHandler->sync(this);

      _la = _input->LA(1);
      if (_la == CypherParser::SP) {
        setState(428);
        match(CypherParser::SP);
      }
      setState(431);
      sortItem();
      setState(436);
      _errHandler->sync(this);
      _la = _input->LA(1);
    }

  } catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- SkipContext
//------------------------------------------------------------------

CypherParser::SkipContext::SkipContext(ParserRuleContext *parent,
                                       size_t invokingState)
    : ParserRuleContext(parent, invokingState) {}

tree::TerminalNode *CypherParser::SkipContext::L_SKIP() {
  return getToken(CypherParser::L_SKIP, 0);
}

tree::TerminalNode *CypherParser::SkipContext::SP() {
  return getToken(CypherParser::SP, 0);
}

CypherParser::ExpressionContext *CypherParser::SkipContext::expression() {
  return getRuleContext<CypherParser::ExpressionContext>(0);
}

size_t CypherParser::SkipContext::getRuleIndex() const {
  return CypherParser::RuleSkip;
}

void CypherParser::SkipContext::enterRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<CypherListener *>(listener);
  if (parserListener != nullptr) parserListener->enterSkip(this);
}

void CypherParser::SkipContext::exitRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<CypherListener *>(listener);
  if (parserListener != nullptr) parserListener->exitSkip(this);
}

antlrcpp::Any CypherParser::SkipContext::accept(
    tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<CypherVisitor *>(visitor))
    return parserVisitor->visitSkip(this);
  else
    return visitor->visitChildren(this);
}

CypherParser::SkipContext *CypherParser::skip() {
  SkipContext *_localctx =
      _tracker.createInstance<SkipContext>(_ctx, getState());
  enterRule(_localctx, 46, CypherParser::RuleSkip);

  auto onExit = finally([=] { exitRule(); });
  try {
    enterOuterAlt(_localctx, 1);
    setState(437);
    match(CypherParser::L_SKIP);
    setState(438);
    match(CypherParser::SP);
    setState(439);
    expression();

  } catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- LimitContext
//------------------------------------------------------------------

CypherParser::LimitContext::LimitContext(ParserRuleContext *parent,
                                         size_t invokingState)
    : ParserRuleContext(parent, invokingState) {}

tree::TerminalNode *CypherParser::LimitContext::LIMIT() {
  return getToken(CypherParser::LIMIT, 0);
}

tree::TerminalNode *CypherParser::LimitContext::SP() {
  return getToken(CypherParser::SP, 0);
}

CypherParser::ExpressionContext *CypherParser::LimitContext::expression() {
  return getRuleContext<CypherParser::ExpressionContext>(0);
}

size_t CypherParser::LimitContext::getRuleIndex() const {
  return CypherParser::RuleLimit;
}

void CypherParser::LimitContext::enterRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<CypherListener *>(listener);
  if (parserListener != nullptr) parserListener->enterLimit(this);
}

void CypherParser::LimitContext::exitRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<CypherListener *>(listener);
  if (parserListener != nullptr) parserListener->exitLimit(this);
}

antlrcpp::Any CypherParser::LimitContext::accept(
    tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<CypherVisitor *>(visitor))
    return parserVisitor->visitLimit(this);
  else
    return visitor->visitChildren(this);
}

CypherParser::LimitContext *CypherParser::limit() {
  LimitContext *_localctx =
      _tracker.createInstance<LimitContext>(_ctx, getState());
  enterRule(_localctx, 48, CypherParser::RuleLimit);

  auto onExit = finally([=] { exitRule(); });
  try {
    enterOuterAlt(_localctx, 1);
    setState(441);
    match(CypherParser::LIMIT);
    setState(442);
    match(CypherParser::SP);
    setState(443);
    expression();

  } catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- SortItemContext
//------------------------------------------------------------------

CypherParser::SortItemContext::SortItemContext(ParserRuleContext *parent,
                                               size_t invokingState)
    : ParserRuleContext(parent, invokingState) {}

CypherParser::ExpressionContext *CypherParser::SortItemContext::expression() {
  return getRuleContext<CypherParser::ExpressionContext>(0);
}

tree::TerminalNode *CypherParser::SortItemContext::ASCENDING() {
  return getToken(CypherParser::ASCENDING, 0);
}

tree::TerminalNode *CypherParser::SortItemContext::ASC() {
  return getToken(CypherParser::ASC, 0);
}

tree::TerminalNode *CypherParser::SortItemContext::DESCENDING() {
  return getToken(CypherParser::DESCENDING, 0);
}

tree::TerminalNode *CypherParser::SortItemContext::DESC() {
  return getToken(CypherParser::DESC, 0);
}

tree::TerminalNode *CypherParser::SortItemContext::SP() {
  return getToken(CypherParser::SP, 0);
}

size_t CypherParser::SortItemContext::getRuleIndex() const {
  return CypherParser::RuleSortItem;
}

void CypherParser::SortItemContext::enterRule(
    tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<CypherListener *>(listener);
  if (parserListener != nullptr) parserListener->enterSortItem(this);
}

void CypherParser::SortItemContext::exitRule(
    tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<CypherListener *>(listener);
  if (parserListener != nullptr) parserListener->exitSortItem(this);
}

antlrcpp::Any CypherParser::SortItemContext::accept(
    tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<CypherVisitor *>(visitor))
    return parserVisitor->visitSortItem(this);
  else
    return visitor->visitChildren(this);
}

CypherParser::SortItemContext *CypherParser::sortItem() {
  SortItemContext *_localctx =
      _tracker.createInstance<SortItemContext>(_ctx, getState());
  enterRule(_localctx, 50, CypherParser::RuleSortItem);
  size_t _la = 0;

  auto onExit = finally([=] { exitRule(); });
  try {
    enterOuterAlt(_localctx, 1);
    setState(445);
    expression();
    setState(450);
    _errHandler->sync(this);

    switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(
        _input, 52, _ctx)) {
      case 1: {
        setState(447);
        _errHandler->sync(this);

        _la = _input->LA(1);
        if (_la == CypherParser::SP) {
          setState(446);
          match(CypherParser::SP);
        }
        setState(449);
        _la = _input->LA(1);
        if (!(((((_la - 84) & ~0x3fULL) == 0) &&
               ((1ULL << (_la - 84)) &
                ((1ULL << (CypherParser::ASCENDING - 84)) |
                 (1ULL << (CypherParser::ASC - 84)) |
                 (1ULL << (CypherParser::DESCENDING - 84)) |
                 (1ULL << (CypherParser::DESC - 84)))) != 0))) {
          _errHandler->recoverInline(this);
        } else {
          _errHandler->reportMatch(this);
          consume();
        }
        break;
      }
    }

  } catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- WhereContext
//------------------------------------------------------------------

CypherParser::WhereContext::WhereContext(ParserRuleContext *parent,
                                         size_t invokingState)
    : ParserRuleContext(parent, invokingState) {}

tree::TerminalNode *CypherParser::WhereContext::WHERE() {
  return getToken(CypherParser::WHERE, 0);
}

tree::TerminalNode *CypherParser::WhereContext::SP() {
  return getToken(CypherParser::SP, 0);
}

CypherParser::ExpressionContext *CypherParser::WhereContext::expression() {
  return getRuleContext<CypherParser::ExpressionContext>(0);
}

size_t CypherParser::WhereContext::getRuleIndex() const {
  return CypherParser::RuleWhere;
}

void CypherParser::WhereContext::enterRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<CypherListener *>(listener);
  if (parserListener != nullptr) parserListener->enterWhere(this);
}

void CypherParser::WhereContext::exitRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<CypherListener *>(listener);
  if (parserListener != nullptr) parserListener->exitWhere(this);
}

antlrcpp::Any CypherParser::WhereContext::accept(
    tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<CypherVisitor *>(visitor))
    return parserVisitor->visitWhere(this);
  else
    return visitor->visitChildren(this);
}

CypherParser::WhereContext *CypherParser::where() {
  WhereContext *_localctx =
      _tracker.createInstance<WhereContext>(_ctx, getState());
  enterRule(_localctx, 52, CypherParser::RuleWhere);

  auto onExit = finally([=] { exitRule(); });
  try {
    enterOuterAlt(_localctx, 1);
    setState(452);
    match(CypherParser::WHERE);
    setState(453);
    match(CypherParser::SP);
    setState(454);
    expression();

  } catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- PatternContext
//------------------------------------------------------------------

CypherParser::PatternContext::PatternContext(ParserRuleContext *parent,
                                             size_t invokingState)
    : ParserRuleContext(parent, invokingState) {}

std::vector<CypherParser::PatternPartContext *>
CypherParser::PatternContext::patternPart() {
  return getRuleContexts<CypherParser::PatternPartContext>();
}

CypherParser::PatternPartContext *CypherParser::PatternContext::patternPart(
    size_t i) {
  return getRuleContext<CypherParser::PatternPartContext>(i);
}

std::vector<tree::TerminalNode *> CypherParser::PatternContext::SP() {
  return getTokens(CypherParser::SP);
}

tree::TerminalNode *CypherParser::PatternContext::SP(size_t i) {
  return getToken(CypherParser::SP, i);
}

size_t CypherParser::PatternContext::getRuleIndex() const {
  return CypherParser::RulePattern;
}

void CypherParser::PatternContext::enterRule(
    tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<CypherListener *>(listener);
  if (parserListener != nullptr) parserListener->enterPattern(this);
}

void CypherParser::PatternContext::exitRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<CypherListener *>(listener);
  if (parserListener != nullptr) parserListener->exitPattern(this);
}

antlrcpp::Any CypherParser::PatternContext::accept(
    tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<CypherVisitor *>(visitor))
    return parserVisitor->visitPattern(this);
  else
    return visitor->visitChildren(this);
}

CypherParser::PatternContext *CypherParser::pattern() {
  PatternContext *_localctx =
      _tracker.createInstance<PatternContext>(_ctx, getState());
  enterRule(_localctx, 54, CypherParser::RulePattern);
  size_t _la = 0;

  auto onExit = finally([=] { exitRule(); });
  try {
    size_t alt;
    enterOuterAlt(_localctx, 1);
    setState(456);
    patternPart();
    setState(467);
    _errHandler->sync(this);
    alt = getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 55,
                                                                     _ctx);
    while (alt != 2 && alt != atn::ATN::INVALID_ALT_NUMBER) {
      if (alt == 1) {
        setState(458);
        _errHandler->sync(this);

        _la = _input->LA(1);
        if (_la == CypherParser::SP) {
          setState(457);
          match(CypherParser::SP);
        }
        setState(460);
        match(CypherParser::T__1);
        setState(462);
        _errHandler->sync(this);

        _la = _input->LA(1);
        if (_la == CypherParser::SP) {
          setState(461);
          match(CypherParser::SP);
        }
        setState(464);
        patternPart();
      }
      setState(469);
      _errHandler->sync(this);
      alt = getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(
          _input, 55, _ctx);
    }

  } catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- PatternPartContext
//------------------------------------------------------------------

CypherParser::PatternPartContext::PatternPartContext(ParserRuleContext *parent,
                                                     size_t invokingState)
    : ParserRuleContext(parent, invokingState) {}

CypherParser::VariableContext *CypherParser::PatternPartContext::variable() {
  return getRuleContext<CypherParser::VariableContext>(0);
}

CypherParser::AnonymousPatternPartContext *
CypherParser::PatternPartContext::anonymousPatternPart() {
  return getRuleContext<CypherParser::AnonymousPatternPartContext>(0);
}

std::vector<tree::TerminalNode *> CypherParser::PatternPartContext::SP() {
  return getTokens(CypherParser::SP);
}

tree::TerminalNode *CypherParser::PatternPartContext::SP(size_t i) {
  return getToken(CypherParser::SP, i);
}

size_t CypherParser::PatternPartContext::getRuleIndex() const {
  return CypherParser::RulePatternPart;
}

void CypherParser::PatternPartContext::enterRule(
    tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<CypherListener *>(listener);
  if (parserListener != nullptr) parserListener->enterPatternPart(this);
}

void CypherParser::PatternPartContext::exitRule(
    tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<CypherListener *>(listener);
  if (parserListener != nullptr) parserListener->exitPatternPart(this);
}

antlrcpp::Any CypherParser::PatternPartContext::accept(
    tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<CypherVisitor *>(visitor))
    return parserVisitor->visitPatternPart(this);
  else
    return visitor->visitChildren(this);
}

CypherParser::PatternPartContext *CypherParser::patternPart() {
  PatternPartContext *_localctx =
      _tracker.createInstance<PatternPartContext>(_ctx, getState());
  enterRule(_localctx, 56, CypherParser::RulePatternPart);
  size_t _la = 0;

  auto onExit = finally([=] { exitRule(); });
  try {
    setState(481);
    _errHandler->sync(this);
    switch (_input->LA(1)) {
      case CypherParser::HexLetter:
      case CypherParser::UNION:
      case CypherParser::ALL:
      case CypherParser::OPTIONAL:
      case CypherParser::MATCH:
      case CypherParser::UNWIND:
      case CypherParser::AS:
      case CypherParser::MERGE:
      case CypherParser::ON:
      case CypherParser::CREATE:
      case CypherParser::SET:
      case CypherParser::DETACH:
      case CypherParser::DELETE:
      case CypherParser::REMOVE:
      case CypherParser::WITH:
      case CypherParser::DISTINCT:
      case CypherParser::RETURN:
      case CypherParser::ORDER:
      case CypherParser::BY:
      case CypherParser::L_SKIP:
      case CypherParser::LIMIT:
      case CypherParser::ASCENDING:
      case CypherParser::ASC:
      case CypherParser::DESCENDING:
      case CypherParser::DESC:
      case CypherParser::WHERE:
      case CypherParser::OR:
      case CypherParser::XOR:
      case CypherParser::AND:
      case CypherParser::NOT:
      case CypherParser::IN:
      case CypherParser::STARTS:
      case CypherParser::ENDS:
      case CypherParser::CONTAINS:
      case CypherParser::IS:
      case CypherParser::CYPHERNULL:
      case CypherParser::COUNT:
      case CypherParser::FILTER:
      case CypherParser::EXTRACT:
      case CypherParser::ANY:
      case CypherParser::NONE:
      case CypherParser::SINGLE:
      case CypherParser::TRUE:
      case CypherParser::FALSE:
      case CypherParser::UnescapedSymbolicName:
      case CypherParser::EscapedSymbolicName: {
        enterOuterAlt(_localctx, 1);
        setState(470);
        variable();
        setState(472);
        _errHandler->sync(this);

        _la = _input->LA(1);
        if (_la == CypherParser::SP) {
          setState(471);
          match(CypherParser::SP);
        }
        setState(474);
        match(CypherParser::T__2);
        setState(476);
        _errHandler->sync(this);

        _la = _input->LA(1);
        if (_la == CypherParser::SP) {
          setState(475);
          match(CypherParser::SP);
        }
        setState(478);
        anonymousPatternPart();
        break;
      }

      case CypherParser::T__5: {
        enterOuterAlt(_localctx, 2);
        setState(480);
        anonymousPatternPart();
        break;
      }

      default:
        throw NoViableAltException(this);
    }

  } catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- AnonymousPatternPartContext
//------------------------------------------------------------------

CypherParser::AnonymousPatternPartContext::AnonymousPatternPartContext(
    ParserRuleContext *parent, size_t invokingState)
    : ParserRuleContext(parent, invokingState) {}

CypherParser::PatternElementContext *
CypherParser::AnonymousPatternPartContext::patternElement() {
  return getRuleContext<CypherParser::PatternElementContext>(0);
}

size_t CypherParser::AnonymousPatternPartContext::getRuleIndex() const {
  return CypherParser::RuleAnonymousPatternPart;
}

void CypherParser::AnonymousPatternPartContext::enterRule(
    tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<CypherListener *>(listener);
  if (parserListener != nullptr)
    parserListener->enterAnonymousPatternPart(this);
}

void CypherParser::AnonymousPatternPartContext::exitRule(
    tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<CypherListener *>(listener);
  if (parserListener != nullptr) parserListener->exitAnonymousPatternPart(this);
}

antlrcpp::Any CypherParser::AnonymousPatternPartContext::accept(
    tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<CypherVisitor *>(visitor))
    return parserVisitor->visitAnonymousPatternPart(this);
  else
    return visitor->visitChildren(this);
}

CypherParser::AnonymousPatternPartContext *
CypherParser::anonymousPatternPart() {
  AnonymousPatternPartContext *_localctx =
      _tracker.createInstance<AnonymousPatternPartContext>(_ctx, getState());
  enterRule(_localctx, 58, CypherParser::RuleAnonymousPatternPart);

  auto onExit = finally([=] { exitRule(); });
  try {
    enterOuterAlt(_localctx, 1);
    setState(483);
    patternElement();

  } catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- PatternElementContext
//------------------------------------------------------------------

CypherParser::PatternElementContext::PatternElementContext(
    ParserRuleContext *parent, size_t invokingState)
    : ParserRuleContext(parent, invokingState) {}

CypherParser::NodePatternContext *
CypherParser::PatternElementContext::nodePattern() {
  return getRuleContext<CypherParser::NodePatternContext>(0);
}

std::vector<CypherParser::PatternElementChainContext *>
CypherParser::PatternElementContext::patternElementChain() {
  return getRuleContexts<CypherParser::PatternElementChainContext>();
}

CypherParser::PatternElementChainContext *
CypherParser::PatternElementContext::patternElementChain(size_t i) {
  return getRuleContext<CypherParser::PatternElementChainContext>(i);
}

std::vector<tree::TerminalNode *> CypherParser::PatternElementContext::SP() {
  return getTokens(CypherParser::SP);
}

tree::TerminalNode *CypherParser::PatternElementContext::SP(size_t i) {
  return getToken(CypherParser::SP, i);
}

CypherParser::PatternElementContext *
CypherParser::PatternElementContext::patternElement() {
  return getRuleContext<CypherParser::PatternElementContext>(0);
}

size_t CypherParser::PatternElementContext::getRuleIndex() const {
  return CypherParser::RulePatternElement;
}

void CypherParser::PatternElementContext::enterRule(
    tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<CypherListener *>(listener);
  if (parserListener != nullptr) parserListener->enterPatternElement(this);
}

void CypherParser::PatternElementContext::exitRule(
    tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<CypherListener *>(listener);
  if (parserListener != nullptr) parserListener->exitPatternElement(this);
}

antlrcpp::Any CypherParser::PatternElementContext::accept(
    tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<CypherVisitor *>(visitor))
    return parserVisitor->visitPatternElement(this);
  else
    return visitor->visitChildren(this);
}

CypherParser::PatternElementContext *CypherParser::patternElement() {
  PatternElementContext *_localctx =
      _tracker.createInstance<PatternElementContext>(_ctx, getState());
  enterRule(_localctx, 60, CypherParser::RulePatternElement);
  size_t _la = 0;

  auto onExit = finally([=] { exitRule(); });
  try {
    size_t alt;
    setState(499);
    _errHandler->sync(this);
    switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(
        _input, 61, _ctx)) {
      case 1: {
        enterOuterAlt(_localctx, 1);
        setState(485);
        nodePattern();
        setState(492);
        _errHandler->sync(this);
        alt = getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(
            _input, 60, _ctx);
        while (alt != 2 && alt != atn::ATN::INVALID_ALT_NUMBER) {
          if (alt == 1) {
            setState(487);
            _errHandler->sync(this);

            _la = _input->LA(1);
            if (_la == CypherParser::SP) {
              setState(486);
              match(CypherParser::SP);
            }
            setState(489);
            patternElementChain();
          }
          setState(494);
          _errHandler->sync(this);
          alt = getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(
              _input, 60, _ctx);
        }
        break;
      }

      case 2: {
        enterOuterAlt(_localctx, 2);
        setState(495);
        match(CypherParser::T__5);
        setState(496);
        patternElement();
        setState(497);
        match(CypherParser::T__6);
        break;
      }
    }

  } catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- NodePatternContext
//------------------------------------------------------------------

CypherParser::NodePatternContext::NodePatternContext(ParserRuleContext *parent,
                                                     size_t invokingState)
    : ParserRuleContext(parent, invokingState) {}

std::vector<tree::TerminalNode *> CypherParser::NodePatternContext::SP() {
  return getTokens(CypherParser::SP);
}

tree::TerminalNode *CypherParser::NodePatternContext::SP(size_t i) {
  return getToken(CypherParser::SP, i);
}

CypherParser::VariableContext *CypherParser::NodePatternContext::variable() {
  return getRuleContext<CypherParser::VariableContext>(0);
}

CypherParser::NodeLabelsContext *
CypherParser::NodePatternContext::nodeLabels() {
  return getRuleContext<CypherParser::NodeLabelsContext>(0);
}

CypherParser::PropertiesContext *
CypherParser::NodePatternContext::properties() {
  return getRuleContext<CypherParser::PropertiesContext>(0);
}

size_t CypherParser::NodePatternContext::getRuleIndex() const {
  return CypherParser::RuleNodePattern;
}

void CypherParser::NodePatternContext::enterRule(
    tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<CypherListener *>(listener);
  if (parserListener != nullptr) parserListener->enterNodePattern(this);
}

void CypherParser::NodePatternContext::exitRule(
    tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<CypherListener *>(listener);
  if (parserListener != nullptr) parserListener->exitNodePattern(this);
}

antlrcpp::Any CypherParser::NodePatternContext::accept(
    tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<CypherVisitor *>(visitor))
    return parserVisitor->visitNodePattern(this);
  else
    return visitor->visitChildren(this);
}

CypherParser::NodePatternContext *CypherParser::nodePattern() {
  NodePatternContext *_localctx =
      _tracker.createInstance<NodePatternContext>(_ctx, getState());
  enterRule(_localctx, 62, CypherParser::RuleNodePattern);
  size_t _la = 0;

  auto onExit = finally([=] { exitRule(); });
  try {
    enterOuterAlt(_localctx, 1);
    setState(501);
    match(CypherParser::T__5);
    setState(503);
    _errHandler->sync(this);

    _la = _input->LA(1);
    if (_la == CypherParser::SP) {
      setState(502);
      match(CypherParser::SP);
    }
    setState(509);
    _errHandler->sync(this);

    _la = _input->LA(1);
    if (((((_la - 55) & ~0x3fULL) == 0) &&
         ((1ULL << (_la - 55)) &
          ((1ULL << (CypherParser::HexLetter - 55)) |
           (1ULL << (CypherParser::UNION - 55)) |
           (1ULL << (CypherParser::ALL - 55)) |
           (1ULL << (CypherParser::OPTIONAL - 55)) |
           (1ULL << (CypherParser::MATCH - 55)) |
           (1ULL << (CypherParser::UNWIND - 55)) |
           (1ULL << (CypherParser::AS - 55)) |
           (1ULL << (CypherParser::MERGE - 55)) |
           (1ULL << (CypherParser::ON - 55)) |
           (1ULL << (CypherParser::CREATE - 55)) |
           (1ULL << (CypherParser::SET - 55)) |
           (1ULL << (CypherParser::DETACH - 55)) |
           (1ULL << (CypherParser::DELETE - 55)) |
           (1ULL << (CypherParser::REMOVE - 55)) |
           (1ULL << (CypherParser::WITH - 55)) |
           (1ULL << (CypherParser::DISTINCT - 55)) |
           (1ULL << (CypherParser::RETURN - 55)) |
           (1ULL << (CypherParser::ORDER - 55)) |
           (1ULL << (CypherParser::BY - 55)) |
           (1ULL << (CypherParser::L_SKIP - 55)) |
           (1ULL << (CypherParser::LIMIT - 55)) |
           (1ULL << (CypherParser::ASCENDING - 55)) |
           (1ULL << (CypherParser::ASC - 55)) |
           (1ULL << (CypherParser::DESCENDING - 55)) |
           (1ULL << (CypherParser::DESC - 55)) |
           (1ULL << (CypherParser::WHERE - 55)) |
           (1ULL << (CypherParser::OR - 55)) |
           (1ULL << (CypherParser::XOR - 55)) |
           (1ULL << (CypherParser::AND - 55)) |
           (1ULL << (CypherParser::NOT - 55)) |
           (1ULL << (CypherParser::IN - 55)) |
           (1ULL << (CypherParser::STARTS - 55)) |
           (1ULL << (CypherParser::ENDS - 55)) |
           (1ULL << (CypherParser::CONTAINS - 55)) |
           (1ULL << (CypherParser::IS - 55)) |
           (1ULL << (CypherParser::CYPHERNULL - 55)) |
           (1ULL << (CypherParser::COUNT - 55)) |
           (1ULL << (CypherParser::FILTER - 55)) |
           (1ULL << (CypherParser::EXTRACT - 55)) |
           (1ULL << (CypherParser::ANY - 55)) |
           (1ULL << (CypherParser::NONE - 55)) |
           (1ULL << (CypherParser::SINGLE - 55)) |
           (1ULL << (CypherParser::TRUE - 55)) |
           (1ULL << (CypherParser::FALSE - 55)) |
           (1ULL << (CypherParser::UnescapedSymbolicName - 55)) |
           (1ULL << (CypherParser::EscapedSymbolicName - 55)))) != 0)) {
      setState(505);
      variable();
      setState(507);
      _errHandler->sync(this);

      _la = _input->LA(1);
      if (_la == CypherParser::SP) {
        setState(506);
        match(CypherParser::SP);
      }
    }
    setState(515);
    _errHandler->sync(this);

    _la = _input->LA(1);
    if (_la == CypherParser::T__10) {
      setState(511);
      nodeLabels();
      setState(513);
      _errHandler->sync(this);

      _la = _input->LA(1);
      if (_la == CypherParser::SP) {
        setState(512);
        match(CypherParser::SP);
      }
    }
    setState(521);
    _errHandler->sync(this);

    _la = _input->LA(1);
    if (_la == CypherParser::T__27

        || _la == CypherParser::T__29) {
      setState(517);
      properties();
      setState(519);
      _errHandler->sync(this);

      _la = _input->LA(1);
      if (_la == CypherParser::SP) {
        setState(518);
        match(CypherParser::SP);
      }
    }
    setState(523);
    match(CypherParser::T__6);

  } catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- PatternElementChainContext
//------------------------------------------------------------------

CypherParser::PatternElementChainContext::PatternElementChainContext(
    ParserRuleContext *parent, size_t invokingState)
    : ParserRuleContext(parent, invokingState) {}

CypherParser::RelationshipPatternContext *
CypherParser::PatternElementChainContext::relationshipPattern() {
  return getRuleContext<CypherParser::RelationshipPatternContext>(0);
}

CypherParser::NodePatternContext *
CypherParser::PatternElementChainContext::nodePattern() {
  return getRuleContext<CypherParser::NodePatternContext>(0);
}

tree::TerminalNode *CypherParser::PatternElementChainContext::SP() {
  return getToken(CypherParser::SP, 0);
}

size_t CypherParser::PatternElementChainContext::getRuleIndex() const {
  return CypherParser::RulePatternElementChain;
}

void CypherParser::PatternElementChainContext::enterRule(
    tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<CypherListener *>(listener);
  if (parserListener != nullptr) parserListener->enterPatternElementChain(this);
}

void CypherParser::PatternElementChainContext::exitRule(
    tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<CypherListener *>(listener);
  if (parserListener != nullptr) parserListener->exitPatternElementChain(this);
}

antlrcpp::Any CypherParser::PatternElementChainContext::accept(
    tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<CypherVisitor *>(visitor))
    return parserVisitor->visitPatternElementChain(this);
  else
    return visitor->visitChildren(this);
}

CypherParser::PatternElementChainContext *CypherParser::patternElementChain() {
  PatternElementChainContext *_localctx =
      _tracker.createInstance<PatternElementChainContext>(_ctx, getState());
  enterRule(_localctx, 64, CypherParser::RulePatternElementChain);
  size_t _la = 0;

  auto onExit = finally([=] { exitRule(); });
  try {
    enterOuterAlt(_localctx, 1);
    setState(525);
    relationshipPattern();
    setState(527);
    _errHandler->sync(this);

    _la = _input->LA(1);
    if (_la == CypherParser::SP) {
      setState(526);
      match(CypherParser::SP);
    }
    setState(529);
    nodePattern();

  } catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- RelationshipPatternContext
//------------------------------------------------------------------

CypherParser::RelationshipPatternContext::RelationshipPatternContext(
    ParserRuleContext *parent, size_t invokingState)
    : ParserRuleContext(parent, invokingState) {}

CypherParser::LeftArrowHeadContext *
CypherParser::RelationshipPatternContext::leftArrowHead() {
  return getRuleContext<CypherParser::LeftArrowHeadContext>(0);
}

std::vector<CypherParser::DashContext *>
CypherParser::RelationshipPatternContext::dash() {
  return getRuleContexts<CypherParser::DashContext>();
}

CypherParser::DashContext *CypherParser::RelationshipPatternContext::dash(
    size_t i) {
  return getRuleContext<CypherParser::DashContext>(i);
}

CypherParser::RightArrowHeadContext *
CypherParser::RelationshipPatternContext::rightArrowHead() {
  return getRuleContext<CypherParser::RightArrowHeadContext>(0);
}

std::vector<tree::TerminalNode *>
CypherParser::RelationshipPatternContext::SP() {
  return getTokens(CypherParser::SP);
}

tree::TerminalNode *CypherParser::RelationshipPatternContext::SP(size_t i) {
  return getToken(CypherParser::SP, i);
}

CypherParser::RelationshipDetailContext *
CypherParser::RelationshipPatternContext::relationshipDetail() {
  return getRuleContext<CypherParser::RelationshipDetailContext>(0);
}

size_t CypherParser::RelationshipPatternContext::getRuleIndex() const {
  return CypherParser::RuleRelationshipPattern;
}

void CypherParser::RelationshipPatternContext::enterRule(
    tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<CypherListener *>(listener);
  if (parserListener != nullptr) parserListener->enterRelationshipPattern(this);
}

void CypherParser::RelationshipPatternContext::exitRule(
    tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<CypherListener *>(listener);
  if (parserListener != nullptr) parserListener->exitRelationshipPattern(this);
}

antlrcpp::Any CypherParser::RelationshipPatternContext::accept(
    tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<CypherVisitor *>(visitor))
    return parserVisitor->visitRelationshipPattern(this);
  else
    return visitor->visitChildren(this);
}

CypherParser::RelationshipPatternContext *CypherParser::relationshipPattern() {
  RelationshipPatternContext *_localctx =
      _tracker.createInstance<RelationshipPatternContext>(_ctx, getState());
  enterRule(_localctx, 66, CypherParser::RuleRelationshipPattern);
  size_t _la = 0;

  auto onExit = finally([=] { exitRule(); });
  try {
    setState(595);
    _errHandler->sync(this);
    switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(
        _input, 86, _ctx)) {
      case 1: {
        enterOuterAlt(_localctx, 1);
        setState(531);
        leftArrowHead();
        setState(533);
        _errHandler->sync(this);

        _la = _input->LA(1);
        if (_la == CypherParser::SP) {
          setState(532);
          match(CypherParser::SP);
        }
        setState(535);
        dash();
        setState(537);
        _errHandler->sync(this);

        switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(
            _input, 71, _ctx)) {
          case 1: {
            setState(536);
            match(CypherParser::SP);
            break;
          }
        }
        setState(540);
        _errHandler->sync(this);

        _la = _input->LA(1);
        if (_la == CypherParser::T__7) {
          setState(539);
          relationshipDetail();
        }
        setState(543);
        _errHandler->sync(this);

        _la = _input->LA(1);
        if (_la == CypherParser::SP) {
          setState(542);
          match(CypherParser::SP);
        }
        setState(545);
        dash();
        setState(547);
        _errHandler->sync(this);

        _la = _input->LA(1);
        if (_la == CypherParser::SP) {
          setState(546);
          match(CypherParser::SP);
        }
        setState(549);
        rightArrowHead();
        break;
      }

      case 2: {
        enterOuterAlt(_localctx, 2);
        setState(551);
        leftArrowHead();
        setState(553);
        _errHandler->sync(this);

        _la = _input->LA(1);
        if (_la == CypherParser::SP) {
          setState(552);
          match(CypherParser::SP);
        }
        setState(555);
        dash();
        setState(557);
        _errHandler->sync(this);

        switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(
            _input, 76, _ctx)) {
          case 1: {
            setState(556);
            match(CypherParser::SP);
            break;
          }
        }
        setState(560);
        _errHandler->sync(this);

        _la = _input->LA(1);
        if (_la == CypherParser::T__7) {
          setState(559);
          relationshipDetail();
        }
        setState(563);
        _errHandler->sync(this);

        _la = _input->LA(1);
        if (_la == CypherParser::SP) {
          setState(562);
          match(CypherParser::SP);
        }
        setState(565);
        dash();
        break;
      }

      case 3: {
        enterOuterAlt(_localctx, 3);
        setState(567);
        dash();
        setState(569);
        _errHandler->sync(this);

        switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(
            _input, 79, _ctx)) {
          case 1: {
            setState(568);
            match(CypherParser::SP);
            break;
          }
        }
        setState(572);
        _errHandler->sync(this);

        _la = _input->LA(1);
        if (_la == CypherParser::T__7) {
          setState(571);
          relationshipDetail();
        }
        setState(575);
        _errHandler->sync(this);

        _la = _input->LA(1);
        if (_la == CypherParser::SP) {
          setState(574);
          match(CypherParser::SP);
        }
        setState(577);
        dash();
        setState(579);
        _errHandler->sync(this);

        _la = _input->LA(1);
        if (_la == CypherParser::SP) {
          setState(578);
          match(CypherParser::SP);
        }
        setState(581);
        rightArrowHead();
        break;
      }

      case 4: {
        enterOuterAlt(_localctx, 4);
        setState(583);
        dash();
        setState(585);
        _errHandler->sync(this);

        switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(
            _input, 83, _ctx)) {
          case 1: {
            setState(584);
            match(CypherParser::SP);
            break;
          }
        }
        setState(588);
        _errHandler->sync(this);

        _la = _input->LA(1);
        if (_la == CypherParser::T__7) {
          setState(587);
          relationshipDetail();
        }
        setState(591);
        _errHandler->sync(this);

        _la = _input->LA(1);
        if (_la == CypherParser::SP) {
          setState(590);
          match(CypherParser::SP);
        }
        setState(593);
        dash();
        break;
      }
    }

  } catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- RelationshipDetailContext
//------------------------------------------------------------------

CypherParser::RelationshipDetailContext::RelationshipDetailContext(
    ParserRuleContext *parent, size_t invokingState)
    : ParserRuleContext(parent, invokingState) {}

CypherParser::VariableContext *
CypherParser::RelationshipDetailContext::variable() {
  return getRuleContext<CypherParser::VariableContext>(0);
}

CypherParser::RelationshipTypesContext *
CypherParser::RelationshipDetailContext::relationshipTypes() {
  return getRuleContext<CypherParser::RelationshipTypesContext>(0);
}

CypherParser::RangeLiteralContext *
CypherParser::RelationshipDetailContext::rangeLiteral() {
  return getRuleContext<CypherParser::RangeLiteralContext>(0);
}

CypherParser::PropertiesContext *
CypherParser::RelationshipDetailContext::properties() {
  return getRuleContext<CypherParser::PropertiesContext>(0);
}

size_t CypherParser::RelationshipDetailContext::getRuleIndex() const {
  return CypherParser::RuleRelationshipDetail;
}

void CypherParser::RelationshipDetailContext::enterRule(
    tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<CypherListener *>(listener);
  if (parserListener != nullptr) parserListener->enterRelationshipDetail(this);
}

void CypherParser::RelationshipDetailContext::exitRule(
    tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<CypherListener *>(listener);
  if (parserListener != nullptr) parserListener->exitRelationshipDetail(this);
}

antlrcpp::Any CypherParser::RelationshipDetailContext::accept(
    tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<CypherVisitor *>(visitor))
    return parserVisitor->visitRelationshipDetail(this);
  else
    return visitor->visitChildren(this);
}

CypherParser::RelationshipDetailContext *CypherParser::relationshipDetail() {
  RelationshipDetailContext *_localctx =
      _tracker.createInstance<RelationshipDetailContext>(_ctx, getState());
  enterRule(_localctx, 68, CypherParser::RuleRelationshipDetail);
  size_t _la = 0;

  auto onExit = finally([=] { exitRule(); });
  try {
    enterOuterAlt(_localctx, 1);
    setState(597);
    match(CypherParser::T__7);
    setState(599);
    _errHandler->sync(this);

    _la = _input->LA(1);
    if (((((_la - 55) & ~0x3fULL) == 0) &&
         ((1ULL << (_la - 55)) &
          ((1ULL << (CypherParser::HexLetter - 55)) |
           (1ULL << (CypherParser::UNION - 55)) |
           (1ULL << (CypherParser::ALL - 55)) |
           (1ULL << (CypherParser::OPTIONAL - 55)) |
           (1ULL << (CypherParser::MATCH - 55)) |
           (1ULL << (CypherParser::UNWIND - 55)) |
           (1ULL << (CypherParser::AS - 55)) |
           (1ULL << (CypherParser::MERGE - 55)) |
           (1ULL << (CypherParser::ON - 55)) |
           (1ULL << (CypherParser::CREATE - 55)) |
           (1ULL << (CypherParser::SET - 55)) |
           (1ULL << (CypherParser::DETACH - 55)) |
           (1ULL << (CypherParser::DELETE - 55)) |
           (1ULL << (CypherParser::REMOVE - 55)) |
           (1ULL << (CypherParser::WITH - 55)) |
           (1ULL << (CypherParser::DISTINCT - 55)) |
           (1ULL << (CypherParser::RETURN - 55)) |
           (1ULL << (CypherParser::ORDER - 55)) |
           (1ULL << (CypherParser::BY - 55)) |
           (1ULL << (CypherParser::L_SKIP - 55)) |
           (1ULL << (CypherParser::LIMIT - 55)) |
           (1ULL << (CypherParser::ASCENDING - 55)) |
           (1ULL << (CypherParser::ASC - 55)) |
           (1ULL << (CypherParser::DESCENDING - 55)) |
           (1ULL << (CypherParser::DESC - 55)) |
           (1ULL << (CypherParser::WHERE - 55)) |
           (1ULL << (CypherParser::OR - 55)) |
           (1ULL << (CypherParser::XOR - 55)) |
           (1ULL << (CypherParser::AND - 55)) |
           (1ULL << (CypherParser::NOT - 55)) |
           (1ULL << (CypherParser::IN - 55)) |
           (1ULL << (CypherParser::STARTS - 55)) |
           (1ULL << (CypherParser::ENDS - 55)) |
           (1ULL << (CypherParser::CONTAINS - 55)) |
           (1ULL << (CypherParser::IS - 55)) |
           (1ULL << (CypherParser::CYPHERNULL - 55)) |
           (1ULL << (CypherParser::COUNT - 55)) |
           (1ULL << (CypherParser::FILTER - 55)) |
           (1ULL << (CypherParser::EXTRACT - 55)) |
           (1ULL << (CypherParser::ANY - 55)) |
           (1ULL << (CypherParser::NONE - 55)) |
           (1ULL << (CypherParser::SINGLE - 55)) |
           (1ULL << (CypherParser::TRUE - 55)) |
           (1ULL << (CypherParser::FALSE - 55)) |
           (1ULL << (CypherParser::UnescapedSymbolicName - 55)) |
           (1ULL << (CypherParser::EscapedSymbolicName - 55)))) != 0)) {
      setState(598);
      variable();
    }
    setState(602);
    _errHandler->sync(this);

    _la = _input->LA(1);
    if (_la == CypherParser::T__8) {
      setState(601);
      match(CypherParser::T__8);
    }
    setState(605);
    _errHandler->sync(this);

    _la = _input->LA(1);
    if (_la == CypherParser::T__10) {
      setState(604);
      relationshipTypes();
    }
    setState(608);
    _errHandler->sync(this);

    _la = _input->LA(1);
    if (_la == CypherParser::T__4) {
      setState(607);
      rangeLiteral();
    }
    setState(611);
    _errHandler->sync(this);

    _la = _input->LA(1);
    if (_la == CypherParser::T__27

        || _la == CypherParser::T__29) {
      setState(610);
      properties();
    }
    setState(613);
    match(CypherParser::T__9);

  } catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- PropertiesContext
//------------------------------------------------------------------

CypherParser::PropertiesContext::PropertiesContext(ParserRuleContext *parent,
                                                   size_t invokingState)
    : ParserRuleContext(parent, invokingState) {}

CypherParser::MapLiteralContext *CypherParser::PropertiesContext::mapLiteral() {
  return getRuleContext<CypherParser::MapLiteralContext>(0);
}

CypherParser::ParameterContext *CypherParser::PropertiesContext::parameter() {
  return getRuleContext<CypherParser::ParameterContext>(0);
}

size_t CypherParser::PropertiesContext::getRuleIndex() const {
  return CypherParser::RuleProperties;
}

void CypherParser::PropertiesContext::enterRule(
    tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<CypherListener *>(listener);
  if (parserListener != nullptr) parserListener->enterProperties(this);
}

void CypherParser::PropertiesContext::exitRule(
    tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<CypherListener *>(listener);
  if (parserListener != nullptr) parserListener->exitProperties(this);
}

antlrcpp::Any CypherParser::PropertiesContext::accept(
    tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<CypherVisitor *>(visitor))
    return parserVisitor->visitProperties(this);
  else
    return visitor->visitChildren(this);
}

CypherParser::PropertiesContext *CypherParser::properties() {
  PropertiesContext *_localctx =
      _tracker.createInstance<PropertiesContext>(_ctx, getState());
  enterRule(_localctx, 70, CypherParser::RuleProperties);

  auto onExit = finally([=] { exitRule(); });
  try {
    setState(617);
    _errHandler->sync(this);
    switch (_input->LA(1)) {
      case CypherParser::T__27: {
        enterOuterAlt(_localctx, 1);
        setState(615);
        mapLiteral();
        break;
      }

      case CypherParser::T__29: {
        enterOuterAlt(_localctx, 2);
        setState(616);
        parameter();
        break;
      }

      default:
        throw NoViableAltException(this);
    }

  } catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- RelationshipTypesContext
//------------------------------------------------------------------

CypherParser::RelationshipTypesContext::RelationshipTypesContext(
    ParserRuleContext *parent, size_t invokingState)
    : ParserRuleContext(parent, invokingState) {}

std::vector<CypherParser::RelTypeNameContext *>
CypherParser::RelationshipTypesContext::relTypeName() {
  return getRuleContexts<CypherParser::RelTypeNameContext>();
}

CypherParser::RelTypeNameContext *
CypherParser::RelationshipTypesContext::relTypeName(size_t i) {
  return getRuleContext<CypherParser::RelTypeNameContext>(i);
}

std::vector<tree::TerminalNode *> CypherParser::RelationshipTypesContext::SP() {
  return getTokens(CypherParser::SP);
}

tree::TerminalNode *CypherParser::RelationshipTypesContext::SP(size_t i) {
  return getToken(CypherParser::SP, i);
}

size_t CypherParser::RelationshipTypesContext::getRuleIndex() const {
  return CypherParser::RuleRelationshipTypes;
}

void CypherParser::RelationshipTypesContext::enterRule(
    tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<CypherListener *>(listener);
  if (parserListener != nullptr) parserListener->enterRelationshipTypes(this);
}

void CypherParser::RelationshipTypesContext::exitRule(
    tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<CypherListener *>(listener);
  if (parserListener != nullptr) parserListener->exitRelationshipTypes(this);
}

antlrcpp::Any CypherParser::RelationshipTypesContext::accept(
    tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<CypherVisitor *>(visitor))
    return parserVisitor->visitRelationshipTypes(this);
  else
    return visitor->visitChildren(this);
}

CypherParser::RelationshipTypesContext *CypherParser::relationshipTypes() {
  RelationshipTypesContext *_localctx =
      _tracker.createInstance<RelationshipTypesContext>(_ctx, getState());
  enterRule(_localctx, 72, CypherParser::RuleRelationshipTypes);
  size_t _la = 0;

  auto onExit = finally([=] { exitRule(); });
  try {
    enterOuterAlt(_localctx, 1);
    setState(619);
    match(CypherParser::T__10);
    setState(620);
    relTypeName();
    setState(634);
    _errHandler->sync(this);
    _la = _input->LA(1);
    while (_la == CypherParser::T__11 || _la == CypherParser::SP) {
      setState(622);
      _errHandler->sync(this);

      _la = _input->LA(1);
      if (_la == CypherParser::SP) {
        setState(621);
        match(CypherParser::SP);
      }
      setState(624);
      match(CypherParser::T__11);
      setState(626);
      _errHandler->sync(this);

      _la = _input->LA(1);
      if (_la == CypherParser::T__10) {
        setState(625);
        match(CypherParser::T__10);
      }
      setState(629);
      _errHandler->sync(this);

      _la = _input->LA(1);
      if (_la == CypherParser::SP) {
        setState(628);
        match(CypherParser::SP);
      }
      setState(631);
      relTypeName();
      setState(636);
      _errHandler->sync(this);
      _la = _input->LA(1);
    }

  } catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- NodeLabelsContext
//------------------------------------------------------------------

CypherParser::NodeLabelsContext::NodeLabelsContext(ParserRuleContext *parent,
                                                   size_t invokingState)
    : ParserRuleContext(parent, invokingState) {}

std::vector<CypherParser::NodeLabelContext *>
CypherParser::NodeLabelsContext::nodeLabel() {
  return getRuleContexts<CypherParser::NodeLabelContext>();
}

CypherParser::NodeLabelContext *CypherParser::NodeLabelsContext::nodeLabel(
    size_t i) {
  return getRuleContext<CypherParser::NodeLabelContext>(i);
}

std::vector<tree::TerminalNode *> CypherParser::NodeLabelsContext::SP() {
  return getTokens(CypherParser::SP);
}

tree::TerminalNode *CypherParser::NodeLabelsContext::SP(size_t i) {
  return getToken(CypherParser::SP, i);
}

size_t CypherParser::NodeLabelsContext::getRuleIndex() const {
  return CypherParser::RuleNodeLabels;
}

void CypherParser::NodeLabelsContext::enterRule(
    tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<CypherListener *>(listener);
  if (parserListener != nullptr) parserListener->enterNodeLabels(this);
}

void CypherParser::NodeLabelsContext::exitRule(
    tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<CypherListener *>(listener);
  if (parserListener != nullptr) parserListener->exitNodeLabels(this);
}

antlrcpp::Any CypherParser::NodeLabelsContext::accept(
    tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<CypherVisitor *>(visitor))
    return parserVisitor->visitNodeLabels(this);
  else
    return visitor->visitChildren(this);
}

CypherParser::NodeLabelsContext *CypherParser::nodeLabels() {
  NodeLabelsContext *_localctx =
      _tracker.createInstance<NodeLabelsContext>(_ctx, getState());
  enterRule(_localctx, 74, CypherParser::RuleNodeLabels);
  size_t _la = 0;

  auto onExit = finally([=] { exitRule(); });
  try {
    size_t alt;
    enterOuterAlt(_localctx, 1);
    setState(637);
    nodeLabel();
    setState(644);
    _errHandler->sync(this);
    alt = getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 98,
                                                                     _ctx);
    while (alt != 2 && alt != atn::ATN::INVALID_ALT_NUMBER) {
      if (alt == 1) {
        setState(639);
        _errHandler->sync(this);

        _la = _input->LA(1);
        if (_la == CypherParser::SP) {
          setState(638);
          match(CypherParser::SP);
        }
        setState(641);
        nodeLabel();
      }
      setState(646);
      _errHandler->sync(this);
      alt = getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(
          _input, 98, _ctx);
    }

  } catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- NodeLabelContext
//------------------------------------------------------------------

CypherParser::NodeLabelContext::NodeLabelContext(ParserRuleContext *parent,
                                                 size_t invokingState)
    : ParserRuleContext(parent, invokingState) {}

CypherParser::LabelNameContext *CypherParser::NodeLabelContext::labelName() {
  return getRuleContext<CypherParser::LabelNameContext>(0);
}

size_t CypherParser::NodeLabelContext::getRuleIndex() const {
  return CypherParser::RuleNodeLabel;
}

void CypherParser::NodeLabelContext::enterRule(
    tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<CypherListener *>(listener);
  if (parserListener != nullptr) parserListener->enterNodeLabel(this);
}

void CypherParser::NodeLabelContext::exitRule(
    tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<CypherListener *>(listener);
  if (parserListener != nullptr) parserListener->exitNodeLabel(this);
}

antlrcpp::Any CypherParser::NodeLabelContext::accept(
    tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<CypherVisitor *>(visitor))
    return parserVisitor->visitNodeLabel(this);
  else
    return visitor->visitChildren(this);
}

CypherParser::NodeLabelContext *CypherParser::nodeLabel() {
  NodeLabelContext *_localctx =
      _tracker.createInstance<NodeLabelContext>(_ctx, getState());
  enterRule(_localctx, 76, CypherParser::RuleNodeLabel);

  auto onExit = finally([=] { exitRule(); });
  try {
    enterOuterAlt(_localctx, 1);
    setState(647);
    match(CypherParser::T__10);
    setState(648);
    labelName();

  } catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- RangeLiteralContext
//------------------------------------------------------------------

CypherParser::RangeLiteralContext::RangeLiteralContext(
    ParserRuleContext *parent, size_t invokingState)
    : ParserRuleContext(parent, invokingState) {}

std::vector<tree::TerminalNode *> CypherParser::RangeLiteralContext::SP() {
  return getTokens(CypherParser::SP);
}

tree::TerminalNode *CypherParser::RangeLiteralContext::SP(size_t i) {
  return getToken(CypherParser::SP, i);
}

std::vector<CypherParser::IntegerLiteralContext *>
CypherParser::RangeLiteralContext::integerLiteral() {
  return getRuleContexts<CypherParser::IntegerLiteralContext>();
}

CypherParser::IntegerLiteralContext *
CypherParser::RangeLiteralContext::integerLiteral(size_t i) {
  return getRuleContext<CypherParser::IntegerLiteralContext>(i);
}

size_t CypherParser::RangeLiteralContext::getRuleIndex() const {
  return CypherParser::RuleRangeLiteral;
}

void CypherParser::RangeLiteralContext::enterRule(
    tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<CypherListener *>(listener);
  if (parserListener != nullptr) parserListener->enterRangeLiteral(this);
}

void CypherParser::RangeLiteralContext::exitRule(
    tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<CypherListener *>(listener);
  if (parserListener != nullptr) parserListener->exitRangeLiteral(this);
}

antlrcpp::Any CypherParser::RangeLiteralContext::accept(
    tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<CypherVisitor *>(visitor))
    return parserVisitor->visitRangeLiteral(this);
  else
    return visitor->visitChildren(this);
}

CypherParser::RangeLiteralContext *CypherParser::rangeLiteral() {
  RangeLiteralContext *_localctx =
      _tracker.createInstance<RangeLiteralContext>(_ctx, getState());
  enterRule(_localctx, 78, CypherParser::RuleRangeLiteral);
  size_t _la = 0;

  auto onExit = finally([=] { exitRule(); });
  try {
    enterOuterAlt(_localctx, 1);
    setState(650);
    match(CypherParser::T__4);
    setState(652);
    _errHandler->sync(this);

    _la = _input->LA(1);
    if (_la == CypherParser::SP) {
      setState(651);
      match(CypherParser::SP);
    }
    setState(658);
    _errHandler->sync(this);

    _la = _input->LA(1);
    if ((((_la & ~0x3fULL) == 0) &&
         ((1ULL << _la) & ((1ULL << CypherParser::HexInteger) |
                           (1ULL << CypherParser::DecimalInteger) |
                           (1ULL << CypherParser::OctalInteger))) != 0)) {
      setState(654);
      integerLiteral();
      setState(656);
      _errHandler->sync(this);

      _la = _input->LA(1);
      if (_la == CypherParser::SP) {
        setState(655);
        match(CypherParser::SP);
      }
    }
    setState(670);
    _errHandler->sync(this);

    _la = _input->LA(1);
    if (_la == CypherParser::T__12) {
      setState(660);
      match(CypherParser::T__12);
      setState(662);
      _errHandler->sync(this);

      _la = _input->LA(1);
      if (_la == CypherParser::SP) {
        setState(661);
        match(CypherParser::SP);
      }
      setState(668);
      _errHandler->sync(this);

      _la = _input->LA(1);
      if ((((_la & ~0x3fULL) == 0) &&
           ((1ULL << _la) & ((1ULL << CypherParser::HexInteger) |
                             (1ULL << CypherParser::DecimalInteger) |
                             (1ULL << CypherParser::OctalInteger))) != 0)) {
        setState(664);
        integerLiteral();
        setState(666);
        _errHandler->sync(this);

        _la = _input->LA(1);
        if (_la == CypherParser::SP) {
          setState(665);
          match(CypherParser::SP);
        }
      }
    }

  } catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- LabelNameContext
//------------------------------------------------------------------

CypherParser::LabelNameContext::LabelNameContext(ParserRuleContext *parent,
                                                 size_t invokingState)
    : ParserRuleContext(parent, invokingState) {}

CypherParser::SymbolicNameContext *
CypherParser::LabelNameContext::symbolicName() {
  return getRuleContext<CypherParser::SymbolicNameContext>(0);
}

size_t CypherParser::LabelNameContext::getRuleIndex() const {
  return CypherParser::RuleLabelName;
}

void CypherParser::LabelNameContext::enterRule(
    tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<CypherListener *>(listener);
  if (parserListener != nullptr) parserListener->enterLabelName(this);
}

void CypherParser::LabelNameContext::exitRule(
    tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<CypherListener *>(listener);
  if (parserListener != nullptr) parserListener->exitLabelName(this);
}

antlrcpp::Any CypherParser::LabelNameContext::accept(
    tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<CypherVisitor *>(visitor))
    return parserVisitor->visitLabelName(this);
  else
    return visitor->visitChildren(this);
}

CypherParser::LabelNameContext *CypherParser::labelName() {
  LabelNameContext *_localctx =
      _tracker.createInstance<LabelNameContext>(_ctx, getState());
  enterRule(_localctx, 80, CypherParser::RuleLabelName);

  auto onExit = finally([=] { exitRule(); });
  try {
    enterOuterAlt(_localctx, 1);
    setState(672);
    symbolicName();

  } catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- RelTypeNameContext
//------------------------------------------------------------------

CypherParser::RelTypeNameContext::RelTypeNameContext(ParserRuleContext *parent,
                                                     size_t invokingState)
    : ParserRuleContext(parent, invokingState) {}

CypherParser::SymbolicNameContext *
CypherParser::RelTypeNameContext::symbolicName() {
  return getRuleContext<CypherParser::SymbolicNameContext>(0);
}

size_t CypherParser::RelTypeNameContext::getRuleIndex() const {
  return CypherParser::RuleRelTypeName;
}

void CypherParser::RelTypeNameContext::enterRule(
    tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<CypherListener *>(listener);
  if (parserListener != nullptr) parserListener->enterRelTypeName(this);
}

void CypherParser::RelTypeNameContext::exitRule(
    tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<CypherListener *>(listener);
  if (parserListener != nullptr) parserListener->exitRelTypeName(this);
}

antlrcpp::Any CypherParser::RelTypeNameContext::accept(
    tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<CypherVisitor *>(visitor))
    return parserVisitor->visitRelTypeName(this);
  else
    return visitor->visitChildren(this);
}

CypherParser::RelTypeNameContext *CypherParser::relTypeName() {
  RelTypeNameContext *_localctx =
      _tracker.createInstance<RelTypeNameContext>(_ctx, getState());
  enterRule(_localctx, 82, CypherParser::RuleRelTypeName);

  auto onExit = finally([=] { exitRule(); });
  try {
    enterOuterAlt(_localctx, 1);
    setState(674);
    symbolicName();

  } catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- ExpressionContext
//------------------------------------------------------------------

CypherParser::ExpressionContext::ExpressionContext(ParserRuleContext *parent,
                                                   size_t invokingState)
    : ParserRuleContext(parent, invokingState) {}

CypherParser::Expression12Context *
CypherParser::ExpressionContext::expression12() {
  return getRuleContext<CypherParser::Expression12Context>(0);
}

size_t CypherParser::ExpressionContext::getRuleIndex() const {
  return CypherParser::RuleExpression;
}

void CypherParser::ExpressionContext::enterRule(
    tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<CypherListener *>(listener);
  if (parserListener != nullptr) parserListener->enterExpression(this);
}

void CypherParser::ExpressionContext::exitRule(
    tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<CypherListener *>(listener);
  if (parserListener != nullptr) parserListener->exitExpression(this);
}

antlrcpp::Any CypherParser::ExpressionContext::accept(
    tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<CypherVisitor *>(visitor))
    return parserVisitor->visitExpression(this);
  else
    return visitor->visitChildren(this);
}

CypherParser::ExpressionContext *CypherParser::expression() {
  ExpressionContext *_localctx =
      _tracker.createInstance<ExpressionContext>(_ctx, getState());
  enterRule(_localctx, 84, CypherParser::RuleExpression);

  auto onExit = finally([=] { exitRule(); });
  try {
    enterOuterAlt(_localctx, 1);
    setState(676);
    expression12();

  } catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- Expression12Context
//------------------------------------------------------------------

CypherParser::Expression12Context::Expression12Context(
    ParserRuleContext *parent, size_t invokingState)
    : ParserRuleContext(parent, invokingState) {}

std::vector<CypherParser::Expression11Context *>
CypherParser::Expression12Context::expression11() {
  return getRuleContexts<CypherParser::Expression11Context>();
}

CypherParser::Expression11Context *
CypherParser::Expression12Context::expression11(size_t i) {
  return getRuleContext<CypherParser::Expression11Context>(i);
}

std::vector<tree::TerminalNode *> CypherParser::Expression12Context::SP() {
  return getTokens(CypherParser::SP);
}

tree::TerminalNode *CypherParser::Expression12Context::SP(size_t i) {
  return getToken(CypherParser::SP, i);
}

std::vector<tree::TerminalNode *> CypherParser::Expression12Context::OR() {
  return getTokens(CypherParser::OR);
}

tree::TerminalNode *CypherParser::Expression12Context::OR(size_t i) {
  return getToken(CypherParser::OR, i);
}

size_t CypherParser::Expression12Context::getRuleIndex() const {
  return CypherParser::RuleExpression12;
}

void CypherParser::Expression12Context::enterRule(
    tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<CypherListener *>(listener);
  if (parserListener != nullptr) parserListener->enterExpression12(this);
}

void CypherParser::Expression12Context::exitRule(
    tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<CypherListener *>(listener);
  if (parserListener != nullptr) parserListener->exitExpression12(this);
}

antlrcpp::Any CypherParser::Expression12Context::accept(
    tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<CypherVisitor *>(visitor))
    return parserVisitor->visitExpression12(this);
  else
    return visitor->visitChildren(this);
}

CypherParser::Expression12Context *CypherParser::expression12() {
  Expression12Context *_localctx =
      _tracker.createInstance<Expression12Context>(_ctx, getState());
  enterRule(_localctx, 86, CypherParser::RuleExpression12);

  auto onExit = finally([=] { exitRule(); });
  try {
    size_t alt;
    enterOuterAlt(_localctx, 1);
    setState(678);
    expression11();
    setState(685);
    _errHandler->sync(this);
    alt = getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input,
                                                                     106, _ctx);
    while (alt != 2 && alt != atn::ATN::INVALID_ALT_NUMBER) {
      if (alt == 1) {
        setState(679);
        match(CypherParser::SP);
        setState(680);
        match(CypherParser::OR);
        setState(681);
        match(CypherParser::SP);
        setState(682);
        expression11();
      }
      setState(687);
      _errHandler->sync(this);
      alt = getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(
          _input, 106, _ctx);
    }

  } catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- Expression11Context
//------------------------------------------------------------------

CypherParser::Expression11Context::Expression11Context(
    ParserRuleContext *parent, size_t invokingState)
    : ParserRuleContext(parent, invokingState) {}

std::vector<CypherParser::Expression10Context *>
CypherParser::Expression11Context::expression10() {
  return getRuleContexts<CypherParser::Expression10Context>();
}

CypherParser::Expression10Context *
CypherParser::Expression11Context::expression10(size_t i) {
  return getRuleContext<CypherParser::Expression10Context>(i);
}

std::vector<tree::TerminalNode *> CypherParser::Expression11Context::SP() {
  return getTokens(CypherParser::SP);
}

tree::TerminalNode *CypherParser::Expression11Context::SP(size_t i) {
  return getToken(CypherParser::SP, i);
}

std::vector<tree::TerminalNode *> CypherParser::Expression11Context::XOR() {
  return getTokens(CypherParser::XOR);
}

tree::TerminalNode *CypherParser::Expression11Context::XOR(size_t i) {
  return getToken(CypherParser::XOR, i);
}

size_t CypherParser::Expression11Context::getRuleIndex() const {
  return CypherParser::RuleExpression11;
}

void CypherParser::Expression11Context::enterRule(
    tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<CypherListener *>(listener);
  if (parserListener != nullptr) parserListener->enterExpression11(this);
}

void CypherParser::Expression11Context::exitRule(
    tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<CypherListener *>(listener);
  if (parserListener != nullptr) parserListener->exitExpression11(this);
}

antlrcpp::Any CypherParser::Expression11Context::accept(
    tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<CypherVisitor *>(visitor))
    return parserVisitor->visitExpression11(this);
  else
    return visitor->visitChildren(this);
}

CypherParser::Expression11Context *CypherParser::expression11() {
  Expression11Context *_localctx =
      _tracker.createInstance<Expression11Context>(_ctx, getState());
  enterRule(_localctx, 88, CypherParser::RuleExpression11);

  auto onExit = finally([=] { exitRule(); });
  try {
    size_t alt;
    enterOuterAlt(_localctx, 1);
    setState(688);
    expression10();
    setState(695);
    _errHandler->sync(this);
    alt = getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input,
                                                                     107, _ctx);
    while (alt != 2 && alt != atn::ATN::INVALID_ALT_NUMBER) {
      if (alt == 1) {
        setState(689);
        match(CypherParser::SP);
        setState(690);
        match(CypherParser::XOR);
        setState(691);
        match(CypherParser::SP);
        setState(692);
        expression10();
      }
      setState(697);
      _errHandler->sync(this);
      alt = getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(
          _input, 107, _ctx);
    }

  } catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- Expression10Context
//------------------------------------------------------------------

CypherParser::Expression10Context::Expression10Context(
    ParserRuleContext *parent, size_t invokingState)
    : ParserRuleContext(parent, invokingState) {}

std::vector<CypherParser::Expression9Context *>
CypherParser::Expression10Context::expression9() {
  return getRuleContexts<CypherParser::Expression9Context>();
}

CypherParser::Expression9Context *
CypherParser::Expression10Context::expression9(size_t i) {
  return getRuleContext<CypherParser::Expression9Context>(i);
}

std::vector<tree::TerminalNode *> CypherParser::Expression10Context::SP() {
  return getTokens(CypherParser::SP);
}

tree::TerminalNode *CypherParser::Expression10Context::SP(size_t i) {
  return getToken(CypherParser::SP, i);
}

std::vector<tree::TerminalNode *> CypherParser::Expression10Context::AND() {
  return getTokens(CypherParser::AND);
}

tree::TerminalNode *CypherParser::Expression10Context::AND(size_t i) {
  return getToken(CypherParser::AND, i);
}

size_t CypherParser::Expression10Context::getRuleIndex() const {
  return CypherParser::RuleExpression10;
}

void CypherParser::Expression10Context::enterRule(
    tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<CypherListener *>(listener);
  if (parserListener != nullptr) parserListener->enterExpression10(this);
}

void CypherParser::Expression10Context::exitRule(
    tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<CypherListener *>(listener);
  if (parserListener != nullptr) parserListener->exitExpression10(this);
}

antlrcpp::Any CypherParser::Expression10Context::accept(
    tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<CypherVisitor *>(visitor))
    return parserVisitor->visitExpression10(this);
  else
    return visitor->visitChildren(this);
}

CypherParser::Expression10Context *CypherParser::expression10() {
  Expression10Context *_localctx =
      _tracker.createInstance<Expression10Context>(_ctx, getState());
  enterRule(_localctx, 90, CypherParser::RuleExpression10);

  auto onExit = finally([=] { exitRule(); });
  try {
    size_t alt;
    enterOuterAlt(_localctx, 1);
    setState(698);
    expression9();
    setState(705);
    _errHandler->sync(this);
    alt = getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input,
                                                                     108, _ctx);
    while (alt != 2 && alt != atn::ATN::INVALID_ALT_NUMBER) {
      if (alt == 1) {
        setState(699);
        match(CypherParser::SP);
        setState(700);
        match(CypherParser::AND);
        setState(701);
        match(CypherParser::SP);
        setState(702);
        expression9();
      }
      setState(707);
      _errHandler->sync(this);
      alt = getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(
          _input, 108, _ctx);
    }

  } catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- Expression9Context
//------------------------------------------------------------------

CypherParser::Expression9Context::Expression9Context(ParserRuleContext *parent,
                                                     size_t invokingState)
    : ParserRuleContext(parent, invokingState) {}

CypherParser::Expression8Context *
CypherParser::Expression9Context::expression8() {
  return getRuleContext<CypherParser::Expression8Context>(0);
}

std::vector<tree::TerminalNode *> CypherParser::Expression9Context::NOT() {
  return getTokens(CypherParser::NOT);
}

tree::TerminalNode *CypherParser::Expression9Context::NOT(size_t i) {
  return getToken(CypherParser::NOT, i);
}

std::vector<tree::TerminalNode *> CypherParser::Expression9Context::SP() {
  return getTokens(CypherParser::SP);
}

tree::TerminalNode *CypherParser::Expression9Context::SP(size_t i) {
  return getToken(CypherParser::SP, i);
}

size_t CypherParser::Expression9Context::getRuleIndex() const {
  return CypherParser::RuleExpression9;
}

void CypherParser::Expression9Context::enterRule(
    tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<CypherListener *>(listener);
  if (parserListener != nullptr) parserListener->enterExpression9(this);
}

void CypherParser::Expression9Context::exitRule(
    tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<CypherListener *>(listener);
  if (parserListener != nullptr) parserListener->exitExpression9(this);
}

antlrcpp::Any CypherParser::Expression9Context::accept(
    tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<CypherVisitor *>(visitor))
    return parserVisitor->visitExpression9(this);
  else
    return visitor->visitChildren(this);
}

CypherParser::Expression9Context *CypherParser::expression9() {
  Expression9Context *_localctx =
      _tracker.createInstance<Expression9Context>(_ctx, getState());
  enterRule(_localctx, 92, CypherParser::RuleExpression9);
  size_t _la = 0;

  auto onExit = finally([=] { exitRule(); });
  try {
    size_t alt;
    enterOuterAlt(_localctx, 1);
    setState(714);
    _errHandler->sync(this);
    alt = getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input,
                                                                     110, _ctx);
    while (alt != 2 && alt != atn::ATN::INVALID_ALT_NUMBER) {
      if (alt == 1) {
        setState(708);
        match(CypherParser::NOT);
        setState(710);
        _errHandler->sync(this);

        _la = _input->LA(1);
        if (_la == CypherParser::SP) {
          setState(709);
          match(CypherParser::SP);
        }
      }
      setState(716);
      _errHandler->sync(this);
      alt = getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(
          _input, 110, _ctx);
    }
    setState(717);
    expression8();

  } catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- Expression8Context
//------------------------------------------------------------------

CypherParser::Expression8Context::Expression8Context(ParserRuleContext *parent,
                                                     size_t invokingState)
    : ParserRuleContext(parent, invokingState) {}

CypherParser::Expression7Context *
CypherParser::Expression8Context::expression7() {
  return getRuleContext<CypherParser::Expression7Context>(0);
}

std::vector<CypherParser::PartialComparisonExpressionContext *>
CypherParser::Expression8Context::partialComparisonExpression() {
  return getRuleContexts<CypherParser::PartialComparisonExpressionContext>();
}

CypherParser::PartialComparisonExpressionContext *
CypherParser::Expression8Context::partialComparisonExpression(size_t i) {
  return getRuleContext<CypherParser::PartialComparisonExpressionContext>(i);
}

std::vector<tree::TerminalNode *> CypherParser::Expression8Context::SP() {
  return getTokens(CypherParser::SP);
}

tree::TerminalNode *CypherParser::Expression8Context::SP(size_t i) {
  return getToken(CypherParser::SP, i);
}

size_t CypherParser::Expression8Context::getRuleIndex() const {
  return CypherParser::RuleExpression8;
}

void CypherParser::Expression8Context::enterRule(
    tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<CypherListener *>(listener);
  if (parserListener != nullptr) parserListener->enterExpression8(this);
}

void CypherParser::Expression8Context::exitRule(
    tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<CypherListener *>(listener);
  if (parserListener != nullptr) parserListener->exitExpression8(this);
}

antlrcpp::Any CypherParser::Expression8Context::accept(
    tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<CypherVisitor *>(visitor))
    return parserVisitor->visitExpression8(this);
  else
    return visitor->visitChildren(this);
}

CypherParser::Expression8Context *CypherParser::expression8() {
  Expression8Context *_localctx =
      _tracker.createInstance<Expression8Context>(_ctx, getState());
  enterRule(_localctx, 94, CypherParser::RuleExpression8);
  size_t _la = 0;

  auto onExit = finally([=] { exitRule(); });
  try {
    size_t alt;
    enterOuterAlt(_localctx, 1);
    setState(719);
    expression7();
    setState(726);
    _errHandler->sync(this);
    alt = getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input,
                                                                     112, _ctx);
    while (alt != 2 && alt != atn::ATN::INVALID_ALT_NUMBER) {
      if (alt == 1) {
        setState(721);
        _errHandler->sync(this);

        _la = _input->LA(1);
        if (_la == CypherParser::SP) {
          setState(720);
          match(CypherParser::SP);
        }
        setState(723);
        partialComparisonExpression();
      }
      setState(728);
      _errHandler->sync(this);
      alt = getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(
          _input, 112, _ctx);
    }

  } catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- Expression7Context
//------------------------------------------------------------------

CypherParser::Expression7Context::Expression7Context(ParserRuleContext *parent,
                                                     size_t invokingState)
    : ParserRuleContext(parent, invokingState) {}

std::vector<CypherParser::Expression6Context *>
CypherParser::Expression7Context::expression6() {
  return getRuleContexts<CypherParser::Expression6Context>();
}

CypherParser::Expression6Context *CypherParser::Expression7Context::expression6(
    size_t i) {
  return getRuleContext<CypherParser::Expression6Context>(i);
}

std::vector<tree::TerminalNode *> CypherParser::Expression7Context::SP() {
  return getTokens(CypherParser::SP);
}

tree::TerminalNode *CypherParser::Expression7Context::SP(size_t i) {
  return getToken(CypherParser::SP, i);
}

size_t CypherParser::Expression7Context::getRuleIndex() const {
  return CypherParser::RuleExpression7;
}

void CypherParser::Expression7Context::enterRule(
    tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<CypherListener *>(listener);
  if (parserListener != nullptr) parserListener->enterExpression7(this);
}

void CypherParser::Expression7Context::exitRule(
    tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<CypherListener *>(listener);
  if (parserListener != nullptr) parserListener->exitExpression7(this);
}

antlrcpp::Any CypherParser::Expression7Context::accept(
    tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<CypherVisitor *>(visitor))
    return parserVisitor->visitExpression7(this);
  else
    return visitor->visitChildren(this);
}

CypherParser::Expression7Context *CypherParser::expression7() {
  Expression7Context *_localctx =
      _tracker.createInstance<Expression7Context>(_ctx, getState());
  enterRule(_localctx, 96, CypherParser::RuleExpression7);
  size_t _la = 0;

  auto onExit = finally([=] { exitRule(); });
  try {
    size_t alt;
    enterOuterAlt(_localctx, 1);
    setState(729);
    expression6();
    setState(748);
    _errHandler->sync(this);
    alt = getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input,
                                                                     118, _ctx);
    while (alt != 2 && alt != atn::ATN::INVALID_ALT_NUMBER) {
      if (alt == 1) {
        setState(746);
        _errHandler->sync(this);
        switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(
            _input, 117, _ctx)) {
          case 1: {
            setState(731);
            _errHandler->sync(this);

            _la = _input->LA(1);
            if (_la == CypherParser::SP) {
              setState(730);
              match(CypherParser::SP);
            }
            setState(733);
            match(CypherParser::T__13);
            setState(735);
            _errHandler->sync(this);

            _la = _input->LA(1);
            if (_la == CypherParser::SP) {
              setState(734);
              match(CypherParser::SP);
            }
            setState(737);
            expression6();
            break;
          }

          case 2: {
            setState(739);
            _errHandler->sync(this);

            _la = _input->LA(1);
            if (_la == CypherParser::SP) {
              setState(738);
              match(CypherParser::SP);
            }
            setState(741);
            match(CypherParser::T__14);
            setState(743);
            _errHandler->sync(this);

            _la = _input->LA(1);
            if (_la == CypherParser::SP) {
              setState(742);
              match(CypherParser::SP);
            }
            setState(745);
            expression6();
            break;
          }
        }
      }
      setState(750);
      _errHandler->sync(this);
      alt = getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(
          _input, 118, _ctx);
    }

  } catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- Expression6Context
//------------------------------------------------------------------

CypherParser::Expression6Context::Expression6Context(ParserRuleContext *parent,
                                                     size_t invokingState)
    : ParserRuleContext(parent, invokingState) {}

std::vector<CypherParser::Expression5Context *>
CypherParser::Expression6Context::expression5() {
  return getRuleContexts<CypherParser::Expression5Context>();
}

CypherParser::Expression5Context *CypherParser::Expression6Context::expression5(
    size_t i) {
  return getRuleContext<CypherParser::Expression5Context>(i);
}

std::vector<tree::TerminalNode *> CypherParser::Expression6Context::SP() {
  return getTokens(CypherParser::SP);
}

tree::TerminalNode *CypherParser::Expression6Context::SP(size_t i) {
  return getToken(CypherParser::SP, i);
}

size_t CypherParser::Expression6Context::getRuleIndex() const {
  return CypherParser::RuleExpression6;
}

void CypherParser::Expression6Context::enterRule(
    tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<CypherListener *>(listener);
  if (parserListener != nullptr) parserListener->enterExpression6(this);
}

void CypherParser::Expression6Context::exitRule(
    tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<CypherListener *>(listener);
  if (parserListener != nullptr) parserListener->exitExpression6(this);
}

antlrcpp::Any CypherParser::Expression6Context::accept(
    tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<CypherVisitor *>(visitor))
    return parserVisitor->visitExpression6(this);
  else
    return visitor->visitChildren(this);
}

CypherParser::Expression6Context *CypherParser::expression6() {
  Expression6Context *_localctx =
      _tracker.createInstance<Expression6Context>(_ctx, getState());
  enterRule(_localctx, 98, CypherParser::RuleExpression6);
  size_t _la = 0;

  auto onExit = finally([=] { exitRule(); });
  try {
    size_t alt;
    enterOuterAlt(_localctx, 1);
    setState(751);
    expression5();
    setState(778);
    _errHandler->sync(this);
    alt = getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input,
                                                                     126, _ctx);
    while (alt != 2 && alt != atn::ATN::INVALID_ALT_NUMBER) {
      if (alt == 1) {
        setState(776);
        _errHandler->sync(this);
        switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(
            _input, 125, _ctx)) {
          case 1: {
            setState(753);
            _errHandler->sync(this);

            _la = _input->LA(1);
            if (_la == CypherParser::SP) {
              setState(752);
              match(CypherParser::SP);
            }
            setState(755);
            match(CypherParser::T__4);
            setState(757);
            _errHandler->sync(this);

            _la = _input->LA(1);
            if (_la == CypherParser::SP) {
              setState(756);
              match(CypherParser::SP);
            }
            setState(759);
            expression5();
            break;
          }

          case 2: {
            setState(761);
            _errHandler->sync(this);

            _la = _input->LA(1);
            if (_la == CypherParser::SP) {
              setState(760);
              match(CypherParser::SP);
            }
            setState(763);
            match(CypherParser::T__15);
            setState(765);
            _errHandler->sync(this);

            _la = _input->LA(1);
            if (_la == CypherParser::SP) {
              setState(764);
              match(CypherParser::SP);
            }
            setState(767);
            expression5();
            break;
          }

          case 3: {
            setState(769);
            _errHandler->sync(this);

            _la = _input->LA(1);
            if (_la == CypherParser::SP) {
              setState(768);
              match(CypherParser::SP);
            }
            setState(771);
            match(CypherParser::T__16);
            setState(773);
            _errHandler->sync(this);

            _la = _input->LA(1);
            if (_la == CypherParser::SP) {
              setState(772);
              match(CypherParser::SP);
            }
            setState(775);
            expression5();
            break;
          }
        }
      }
      setState(780);
      _errHandler->sync(this);
      alt = getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(
          _input, 126, _ctx);
    }

  } catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- Expression5Context
//------------------------------------------------------------------

CypherParser::Expression5Context::Expression5Context(ParserRuleContext *parent,
                                                     size_t invokingState)
    : ParserRuleContext(parent, invokingState) {}

std::vector<CypherParser::Expression4Context *>
CypherParser::Expression5Context::expression4() {
  return getRuleContexts<CypherParser::Expression4Context>();
}

CypherParser::Expression4Context *CypherParser::Expression5Context::expression4(
    size_t i) {
  return getRuleContext<CypherParser::Expression4Context>(i);
}

std::vector<tree::TerminalNode *> CypherParser::Expression5Context::SP() {
  return getTokens(CypherParser::SP);
}

tree::TerminalNode *CypherParser::Expression5Context::SP(size_t i) {
  return getToken(CypherParser::SP, i);
}

size_t CypherParser::Expression5Context::getRuleIndex() const {
  return CypherParser::RuleExpression5;
}

void CypherParser::Expression5Context::enterRule(
    tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<CypherListener *>(listener);
  if (parserListener != nullptr) parserListener->enterExpression5(this);
}

void CypherParser::Expression5Context::exitRule(
    tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<CypherListener *>(listener);
  if (parserListener != nullptr) parserListener->exitExpression5(this);
}

antlrcpp::Any CypherParser::Expression5Context::accept(
    tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<CypherVisitor *>(visitor))
    return parserVisitor->visitExpression5(this);
  else
    return visitor->visitChildren(this);
}

CypherParser::Expression5Context *CypherParser::expression5() {
  Expression5Context *_localctx =
      _tracker.createInstance<Expression5Context>(_ctx, getState());
  enterRule(_localctx, 100, CypherParser::RuleExpression5);
  size_t _la = 0;

  auto onExit = finally([=] { exitRule(); });
  try {
    size_t alt;
    enterOuterAlt(_localctx, 1);
    setState(781);
    expression4();
    setState(792);
    _errHandler->sync(this);
    alt = getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input,
                                                                     129, _ctx);
    while (alt != 2 && alt != atn::ATN::INVALID_ALT_NUMBER) {
      if (alt == 1) {
        setState(783);
        _errHandler->sync(this);

        _la = _input->LA(1);
        if (_la == CypherParser::SP) {
          setState(782);
          match(CypherParser::SP);
        }
        setState(785);
        match(CypherParser::T__17);
        setState(787);
        _errHandler->sync(this);

        _la = _input->LA(1);
        if (_la == CypherParser::SP) {
          setState(786);
          match(CypherParser::SP);
        }
        setState(789);
        expression4();
      }
      setState(794);
      _errHandler->sync(this);
      alt = getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(
          _input, 129, _ctx);
    }

  } catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- Expression4Context
//------------------------------------------------------------------

CypherParser::Expression4Context::Expression4Context(ParserRuleContext *parent,
                                                     size_t invokingState)
    : ParserRuleContext(parent, invokingState) {}

CypherParser::Expression3Context *
CypherParser::Expression4Context::expression3() {
  return getRuleContext<CypherParser::Expression3Context>(0);
}

std::vector<tree::TerminalNode *> CypherParser::Expression4Context::SP() {
  return getTokens(CypherParser::SP);
}

tree::TerminalNode *CypherParser::Expression4Context::SP(size_t i) {
  return getToken(CypherParser::SP, i);
}

size_t CypherParser::Expression4Context::getRuleIndex() const {
  return CypherParser::RuleExpression4;
}

void CypherParser::Expression4Context::enterRule(
    tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<CypherListener *>(listener);
  if (parserListener != nullptr) parserListener->enterExpression4(this);
}

void CypherParser::Expression4Context::exitRule(
    tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<CypherListener *>(listener);
  if (parserListener != nullptr) parserListener->exitExpression4(this);
}

antlrcpp::Any CypherParser::Expression4Context::accept(
    tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<CypherVisitor *>(visitor))
    return parserVisitor->visitExpression4(this);
  else
    return visitor->visitChildren(this);
}

CypherParser::Expression4Context *CypherParser::expression4() {
  Expression4Context *_localctx =
      _tracker.createInstance<Expression4Context>(_ctx, getState());
  enterRule(_localctx, 102, CypherParser::RuleExpression4);
  size_t _la = 0;

  auto onExit = finally([=] { exitRule(); });
  try {
    enterOuterAlt(_localctx, 1);
    setState(801);
    _errHandler->sync(this);
    _la = _input->LA(1);
    while (_la == CypherParser::T__13

           || _la == CypherParser::T__14) {
      setState(795);
      _la = _input->LA(1);
      if (!(_la == CypherParser::T__13

            || _la == CypherParser::T__14)) {
        _errHandler->recoverInline(this);
      } else {
        _errHandler->reportMatch(this);
        consume();
      }
      setState(797);
      _errHandler->sync(this);

      _la = _input->LA(1);
      if (_la == CypherParser::SP) {
        setState(796);
        match(CypherParser::SP);
      }
      setState(803);
      _errHandler->sync(this);
      _la = _input->LA(1);
    }
    setState(804);
    expression3();

  } catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- Expression3Context
//------------------------------------------------------------------

CypherParser::Expression3Context::Expression3Context(ParserRuleContext *parent,
                                                     size_t invokingState)
    : ParserRuleContext(parent, invokingState) {}

std::vector<CypherParser::Expression2Context *>
CypherParser::Expression3Context::expression2() {
  return getRuleContexts<CypherParser::Expression2Context>();
}

CypherParser::Expression2Context *CypherParser::Expression3Context::expression2(
    size_t i) {
  return getRuleContext<CypherParser::Expression2Context>(i);
}

std::vector<CypherParser::ExpressionContext *>
CypherParser::Expression3Context::expression() {
  return getRuleContexts<CypherParser::ExpressionContext>();
}

CypherParser::ExpressionContext *CypherParser::Expression3Context::expression(
    size_t i) {
  return getRuleContext<CypherParser::ExpressionContext>(i);
}

std::vector<tree::TerminalNode *> CypherParser::Expression3Context::SP() {
  return getTokens(CypherParser::SP);
}

tree::TerminalNode *CypherParser::Expression3Context::SP(size_t i) {
  return getToken(CypherParser::SP, i);
}

std::vector<tree::TerminalNode *> CypherParser::Expression3Context::IS() {
  return getTokens(CypherParser::IS);
}

tree::TerminalNode *CypherParser::Expression3Context::IS(size_t i) {
  return getToken(CypherParser::IS, i);
}

std::vector<tree::TerminalNode *>
CypherParser::Expression3Context::CYPHERNULL() {
  return getTokens(CypherParser::CYPHERNULL);
}

tree::TerminalNode *CypherParser::Expression3Context::CYPHERNULL(size_t i) {
  return getToken(CypherParser::CYPHERNULL, i);
}

std::vector<tree::TerminalNode *> CypherParser::Expression3Context::NOT() {
  return getTokens(CypherParser::NOT);
}

tree::TerminalNode *CypherParser::Expression3Context::NOT(size_t i) {
  return getToken(CypherParser::NOT, i);
}

std::vector<tree::TerminalNode *> CypherParser::Expression3Context::IN() {
  return getTokens(CypherParser::IN);
}

tree::TerminalNode *CypherParser::Expression3Context::IN(size_t i) {
  return getToken(CypherParser::IN, i);
}

std::vector<tree::TerminalNode *> CypherParser::Expression3Context::STARTS() {
  return getTokens(CypherParser::STARTS);
}

tree::TerminalNode *CypherParser::Expression3Context::STARTS(size_t i) {
  return getToken(CypherParser::STARTS, i);
}

std::vector<tree::TerminalNode *> CypherParser::Expression3Context::WITH() {
  return getTokens(CypherParser::WITH);
}

tree::TerminalNode *CypherParser::Expression3Context::WITH(size_t i) {
  return getToken(CypherParser::WITH, i);
}

std::vector<tree::TerminalNode *> CypherParser::Expression3Context::ENDS() {
  return getTokens(CypherParser::ENDS);
}

tree::TerminalNode *CypherParser::Expression3Context::ENDS(size_t i) {
  return getToken(CypherParser::ENDS, i);
}

std::vector<tree::TerminalNode *> CypherParser::Expression3Context::CONTAINS() {
  return getTokens(CypherParser::CONTAINS);
}

tree::TerminalNode *CypherParser::Expression3Context::CONTAINS(size_t i) {
  return getToken(CypherParser::CONTAINS, i);
}

size_t CypherParser::Expression3Context::getRuleIndex() const {
  return CypherParser::RuleExpression3;
}

void CypherParser::Expression3Context::enterRule(
    tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<CypherListener *>(listener);
  if (parserListener != nullptr) parserListener->enterExpression3(this);
}

void CypherParser::Expression3Context::exitRule(
    tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<CypherListener *>(listener);
  if (parserListener != nullptr) parserListener->exitExpression3(this);
}

antlrcpp::Any CypherParser::Expression3Context::accept(
    tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<CypherVisitor *>(visitor))
    return parserVisitor->visitExpression3(this);
  else
    return visitor->visitChildren(this);
}

CypherParser::Expression3Context *CypherParser::expression3() {
  Expression3Context *_localctx =
      _tracker.createInstance<Expression3Context>(_ctx, getState());
  enterRule(_localctx, 104, CypherParser::RuleExpression3);
  size_t _la = 0;

  auto onExit = finally([=] { exitRule(); });
  try {
    size_t alt;
    enterOuterAlt(_localctx, 1);
    setState(806);
    expression2();
    setState(860);
    _errHandler->sync(this);
    alt = getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input,
                                                                     140, _ctx);
    while (alt != 2 && alt != atn::ATN::INVALID_ALT_NUMBER) {
      if (alt == 1) {
        setState(858);
        _errHandler->sync(this);
        switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(
            _input, 139, _ctx)) {
          case 1: {
            setState(808);
            _errHandler->sync(this);

            _la = _input->LA(1);
            if (_la == CypherParser::SP) {
              setState(807);
              match(CypherParser::SP);
            }
            setState(810);
            match(CypherParser::T__7);
            setState(811);
            expression();
            setState(812);
            match(CypherParser::T__9);
            break;
          }

          case 2: {
            setState(815);
            _errHandler->sync(this);

            _la = _input->LA(1);
            if (_la == CypherParser::SP) {
              setState(814);
              match(CypherParser::SP);
            }
            setState(817);
            match(CypherParser::T__7);
            setState(819);
            _errHandler->sync(this);

            _la = _input->LA(1);
            if ((((_la & ~0x3fULL) == 0) &&
                 ((1ULL << _la) &
                  ((1ULL << CypherParser::T__5) | (1ULL << CypherParser::T__7) |
                   (1ULL << CypherParser::T__13) |
                   (1ULL << CypherParser::T__14) |
                   (1ULL << CypherParser::T__27) |
                   (1ULL << CypherParser::T__29) |
                   (1ULL << CypherParser::StringLiteral) |
                   (1ULL << CypherParser::HexInteger) |
                   (1ULL << CypherParser::DecimalInteger) |
                   (1ULL << CypherParser::OctalInteger) |
                   (1ULL << CypherParser::HexLetter) |
                   (1ULL << CypherParser::ExponentDecimalReal) |
                   (1ULL << CypherParser::RegularDecimalReal))) != 0) ||
                ((((_la - 64) & ~0x3fULL) == 0) &&
                 ((1ULL << (_la - 64)) &
                  ((1ULL << (CypherParser::UNION - 64)) |
                   (1ULL << (CypherParser::ALL - 64)) |
                   (1ULL << (CypherParser::OPTIONAL - 64)) |
                   (1ULL << (CypherParser::MATCH - 64)) |
                   (1ULL << (CypherParser::UNWIND - 64)) |
                   (1ULL << (CypherParser::AS - 64)) |
                   (1ULL << (CypherParser::MERGE - 64)) |
                   (1ULL << (CypherParser::ON - 64)) |
                   (1ULL << (CypherParser::CREATE - 64)) |
                   (1ULL << (CypherParser::SET - 64)) |
                   (1ULL << (CypherParser::DETACH - 64)) |
                   (1ULL << (CypherParser::DELETE - 64)) |
                   (1ULL << (CypherParser::REMOVE - 64)) |
                   (1ULL << (CypherParser::WITH - 64)) |
                   (1ULL << (CypherParser::DISTINCT - 64)) |
                   (1ULL << (CypherParser::RETURN - 64)) |
                   (1ULL << (CypherParser::ORDER - 64)) |
                   (1ULL << (CypherParser::BY - 64)) |
                   (1ULL << (CypherParser::L_SKIP - 64)) |
                   (1ULL << (CypherParser::LIMIT - 64)) |
                   (1ULL << (CypherParser::ASCENDING - 64)) |
                   (1ULL << (CypherParser::ASC - 64)) |
                   (1ULL << (CypherParser::DESCENDING - 64)) |
                   (1ULL << (CypherParser::DESC - 64)) |
                   (1ULL << (CypherParser::WHERE - 64)) |
                   (1ULL << (CypherParser::OR - 64)) |
                   (1ULL << (CypherParser::XOR - 64)) |
                   (1ULL << (CypherParser::AND - 64)) |
                   (1ULL << (CypherParser::NOT - 64)) |
                   (1ULL << (CypherParser::IN - 64)) |
                   (1ULL << (CypherParser::STARTS - 64)) |
                   (1ULL << (CypherParser::ENDS - 64)) |
                   (1ULL << (CypherParser::CONTAINS - 64)) |
                   (1ULL << (CypherParser::IS - 64)) |
                   (1ULL << (CypherParser::CYPHERNULL - 64)) |
                   (1ULL << (CypherParser::COUNT - 64)) |
                   (1ULL << (CypherParser::FILTER - 64)) |
                   (1ULL << (CypherParser::EXTRACT - 64)) |
                   (1ULL << (CypherParser::ANY - 64)) |
                   (1ULL << (CypherParser::NONE - 64)) |
                   (1ULL << (CypherParser::SINGLE - 64)) |
                   (1ULL << (CypherParser::TRUE - 64)) |
                   (1ULL << (CypherParser::FALSE - 64)) |
                   (1ULL << (CypherParser::UnescapedSymbolicName - 64)) |
                   (1ULL << (CypherParser::EscapedSymbolicName - 64)))) != 0)) {
              setState(818);
              expression();
            }
            setState(821);
            match(CypherParser::T__12);
            setState(823);
            _errHandler->sync(this);

            _la = _input->LA(1);
            if ((((_la & ~0x3fULL) == 0) &&
                 ((1ULL << _la) &
                  ((1ULL << CypherParser::T__5) | (1ULL << CypherParser::T__7) |
                   (1ULL << CypherParser::T__13) |
                   (1ULL << CypherParser::T__14) |
                   (1ULL << CypherParser::T__27) |
                   (1ULL << CypherParser::T__29) |
                   (1ULL << CypherParser::StringLiteral) |
                   (1ULL << CypherParser::HexInteger) |
                   (1ULL << CypherParser::DecimalInteger) |
                   (1ULL << CypherParser::OctalInteger) |
                   (1ULL << CypherParser::HexLetter) |
                   (1ULL << CypherParser::ExponentDecimalReal) |
                   (1ULL << CypherParser::RegularDecimalReal))) != 0) ||
                ((((_la - 64) & ~0x3fULL) == 0) &&
                 ((1ULL << (_la - 64)) &
                  ((1ULL << (CypherParser::UNION - 64)) |
                   (1ULL << (CypherParser::ALL - 64)) |
                   (1ULL << (CypherParser::OPTIONAL - 64)) |
                   (1ULL << (CypherParser::MATCH - 64)) |
                   (1ULL << (CypherParser::UNWIND - 64)) |
                   (1ULL << (CypherParser::AS - 64)) |
                   (1ULL << (CypherParser::MERGE - 64)) |
                   (1ULL << (CypherParser::ON - 64)) |
                   (1ULL << (CypherParser::CREATE - 64)) |
                   (1ULL << (CypherParser::SET - 64)) |
                   (1ULL << (CypherParser::DETACH - 64)) |
                   (1ULL << (CypherParser::DELETE - 64)) |
                   (1ULL << (CypherParser::REMOVE - 64)) |
                   (1ULL << (CypherParser::WITH - 64)) |
                   (1ULL << (CypherParser::DISTINCT - 64)) |
                   (1ULL << (CypherParser::RETURN - 64)) |
                   (1ULL << (CypherParser::ORDER - 64)) |
                   (1ULL << (CypherParser::BY - 64)) |
                   (1ULL << (CypherParser::L_SKIP - 64)) |
                   (1ULL << (CypherParser::LIMIT - 64)) |
                   (1ULL << (CypherParser::ASCENDING - 64)) |
                   (1ULL << (CypherParser::ASC - 64)) |
                   (1ULL << (CypherParser::DESCENDING - 64)) |
                   (1ULL << (CypherParser::DESC - 64)) |
                   (1ULL << (CypherParser::WHERE - 64)) |
                   (1ULL << (CypherParser::OR - 64)) |
                   (1ULL << (CypherParser::XOR - 64)) |
                   (1ULL << (CypherParser::AND - 64)) |
                   (1ULL << (CypherParser::NOT - 64)) |
                   (1ULL << (CypherParser::IN - 64)) |
                   (1ULL << (CypherParser::STARTS - 64)) |
                   (1ULL << (CypherParser::ENDS - 64)) |
                   (1ULL << (CypherParser::CONTAINS - 64)) |
                   (1ULL << (CypherParser::IS - 64)) |
                   (1ULL << (CypherParser::CYPHERNULL - 64)) |
                   (1ULL << (CypherParser::COUNT - 64)) |
                   (1ULL << (CypherParser::FILTER - 64)) |
                   (1ULL << (CypherParser::EXTRACT - 64)) |
                   (1ULL << (CypherParser::ANY - 64)) |
                   (1ULL << (CypherParser::NONE - 64)) |
                   (1ULL << (CypherParser::SINGLE - 64)) |
                   (1ULL << (CypherParser::TRUE - 64)) |
                   (1ULL << (CypherParser::FALSE - 64)) |
                   (1ULL << (CypherParser::UnescapedSymbolicName - 64)) |
                   (1ULL << (CypherParser::EscapedSymbolicName - 64)))) != 0)) {
              setState(822);
              expression();
            }
            setState(825);
            match(CypherParser::T__9);
            break;
          }

          case 3: {
            setState(842);
            _errHandler->sync(this);
            switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(
                _input, 137, _ctx)) {
              case 1: {
                setState(827);
                _errHandler->sync(this);

                _la = _input->LA(1);
                if (_la == CypherParser::SP) {
                  setState(826);
                  match(CypherParser::SP);
                }
                setState(829);
                match(CypherParser::T__18);
                break;
              }

              case 2: {
                setState(830);
                match(CypherParser::SP);
                setState(831);
                match(CypherParser::IN);
                break;
              }

              case 3: {
                setState(832);
                match(CypherParser::SP);
                setState(833);
                match(CypherParser::STARTS);
                setState(834);
                match(CypherParser::SP);
                setState(835);
                match(CypherParser::WITH);
                break;
              }

              case 4: {
                setState(836);
                match(CypherParser::SP);
                setState(837);
                match(CypherParser::ENDS);
                setState(838);
                match(CypherParser::SP);
                setState(839);
                match(CypherParser::WITH);
                break;
              }

              case 5: {
                setState(840);
                match(CypherParser::SP);
                setState(841);
                match(CypherParser::CONTAINS);
                break;
              }
            }
            setState(845);
            _errHandler->sync(this);

            _la = _input->LA(1);
            if (_la == CypherParser::SP) {
              setState(844);
              match(CypherParser::SP);
            }
            setState(847);
            expression2();
            break;
          }

          case 4: {
            setState(848);
            match(CypherParser::SP);
            setState(849);
            match(CypherParser::IS);
            setState(850);
            match(CypherParser::SP);
            setState(851);
            match(CypherParser::CYPHERNULL);
            break;
          }

          case 5: {
            setState(852);
            match(CypherParser::SP);
            setState(853);
            match(CypherParser::IS);
            setState(854);
            match(CypherParser::SP);
            setState(855);
            match(CypherParser::NOT);
            setState(856);
            match(CypherParser::SP);
            setState(857);
            match(CypherParser::CYPHERNULL);
            break;
          }
        }
      }
      setState(862);
      _errHandler->sync(this);
      alt = getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(
          _input, 140, _ctx);
    }

  } catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- Expression2Context
//------------------------------------------------------------------

CypherParser::Expression2Context::Expression2Context(ParserRuleContext *parent,
                                                     size_t invokingState)
    : ParserRuleContext(parent, invokingState) {}

CypherParser::AtomContext *CypherParser::Expression2Context::atom() {
  return getRuleContext<CypherParser::AtomContext>(0);
}

std::vector<CypherParser::PropertyLookupContext *>
CypherParser::Expression2Context::propertyLookup() {
  return getRuleContexts<CypherParser::PropertyLookupContext>();
}

CypherParser::PropertyLookupContext *
CypherParser::Expression2Context::propertyLookup(size_t i) {
  return getRuleContext<CypherParser::PropertyLookupContext>(i);
}

std::vector<CypherParser::NodeLabelsContext *>
CypherParser::Expression2Context::nodeLabels() {
  return getRuleContexts<CypherParser::NodeLabelsContext>();
}

CypherParser::NodeLabelsContext *CypherParser::Expression2Context::nodeLabels(
    size_t i) {
  return getRuleContext<CypherParser::NodeLabelsContext>(i);
}

size_t CypherParser::Expression2Context::getRuleIndex() const {
  return CypherParser::RuleExpression2;
}

void CypherParser::Expression2Context::enterRule(
    tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<CypherListener *>(listener);
  if (parserListener != nullptr) parserListener->enterExpression2(this);
}

void CypherParser::Expression2Context::exitRule(
    tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<CypherListener *>(listener);
  if (parserListener != nullptr) parserListener->exitExpression2(this);
}

antlrcpp::Any CypherParser::Expression2Context::accept(
    tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<CypherVisitor *>(visitor))
    return parserVisitor->visitExpression2(this);
  else
    return visitor->visitChildren(this);
}

CypherParser::Expression2Context *CypherParser::expression2() {
  Expression2Context *_localctx =
      _tracker.createInstance<Expression2Context>(_ctx, getState());
  enterRule(_localctx, 106, CypherParser::RuleExpression2);

  auto onExit = finally([=] { exitRule(); });
  try {
    size_t alt;
    enterOuterAlt(_localctx, 1);
    setState(863);
    atom();
    setState(868);
    _errHandler->sync(this);
    alt = getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input,
                                                                     142, _ctx);
    while (alt != 2 && alt != atn::ATN::INVALID_ALT_NUMBER) {
      if (alt == 1) {
        setState(866);
        _errHandler->sync(this);
        switch (_input->LA(1)) {
          case CypherParser::T__25:
          case CypherParser::SP: {
            setState(864);
            propertyLookup();
            break;
          }

          case CypherParser::T__10: {
            setState(865);
            nodeLabels();
            break;
          }

          default:
            throw NoViableAltException(this);
        }
      }
      setState(870);
      _errHandler->sync(this);
      alt = getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(
          _input, 142, _ctx);
    }

  } catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- AtomContext
//------------------------------------------------------------------

CypherParser::AtomContext::AtomContext(ParserRuleContext *parent,
                                       size_t invokingState)
    : ParserRuleContext(parent, invokingState) {}

CypherParser::LiteralContext *CypherParser::AtomContext::literal() {
  return getRuleContext<CypherParser::LiteralContext>(0);
}

CypherParser::ParameterContext *CypherParser::AtomContext::parameter() {
  return getRuleContext<CypherParser::ParameterContext>(0);
}

tree::TerminalNode *CypherParser::AtomContext::COUNT() {
  return getToken(CypherParser::COUNT, 0);
}

std::vector<tree::TerminalNode *> CypherParser::AtomContext::SP() {
  return getTokens(CypherParser::SP);
}

tree::TerminalNode *CypherParser::AtomContext::SP(size_t i) {
  return getToken(CypherParser::SP, i);
}

CypherParser::ListComprehensionContext *
CypherParser::AtomContext::listComprehension() {
  return getRuleContext<CypherParser::ListComprehensionContext>(0);
}

tree::TerminalNode *CypherParser::AtomContext::FILTER() {
  return getToken(CypherParser::FILTER, 0);
}

CypherParser::FilterExpressionContext *
CypherParser::AtomContext::filterExpression() {
  return getRuleContext<CypherParser::FilterExpressionContext>(0);
}

tree::TerminalNode *CypherParser::AtomContext::EXTRACT() {
  return getToken(CypherParser::EXTRACT, 0);
}

CypherParser::ExpressionContext *CypherParser::AtomContext::expression() {
  return getRuleContext<CypherParser::ExpressionContext>(0);
}

tree::TerminalNode *CypherParser::AtomContext::ALL() {
  return getToken(CypherParser::ALL, 0);
}

tree::TerminalNode *CypherParser::AtomContext::ANY() {
  return getToken(CypherParser::ANY, 0);
}

tree::TerminalNode *CypherParser::AtomContext::NONE() {
  return getToken(CypherParser::NONE, 0);
}

tree::TerminalNode *CypherParser::AtomContext::SINGLE() {
  return getToken(CypherParser::SINGLE, 0);
}

CypherParser::RelationshipsPatternContext *
CypherParser::AtomContext::relationshipsPattern() {
  return getRuleContext<CypherParser::RelationshipsPatternContext>(0);
}

CypherParser::ParenthesizedExpressionContext *
CypherParser::AtomContext::parenthesizedExpression() {
  return getRuleContext<CypherParser::ParenthesizedExpressionContext>(0);
}

CypherParser::FunctionInvocationContext *
CypherParser::AtomContext::functionInvocation() {
  return getRuleContext<CypherParser::FunctionInvocationContext>(0);
}

CypherParser::VariableContext *CypherParser::AtomContext::variable() {
  return getRuleContext<CypherParser::VariableContext>(0);
}

size_t CypherParser::AtomContext::getRuleIndex() const {
  return CypherParser::RuleAtom;
}

void CypherParser::AtomContext::enterRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<CypherListener *>(listener);
  if (parserListener != nullptr) parserListener->enterAtom(this);
}

void CypherParser::AtomContext::exitRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<CypherListener *>(listener);
  if (parserListener != nullptr) parserListener->exitAtom(this);
}

antlrcpp::Any CypherParser::AtomContext::accept(
    tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<CypherVisitor *>(visitor))
    return parserVisitor->visitAtom(this);
  else
    return visitor->visitChildren(this);
}

CypherParser::AtomContext *CypherParser::atom() {
  AtomContext *_localctx =
      _tracker.createInstance<AtomContext>(_ctx, getState());
  enterRule(_localctx, 108, CypherParser::RuleAtom);
  size_t _la = 0;

  auto onExit = finally([=] { exitRule(); });
  try {
    setState(982);
    _errHandler->sync(this);
    switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(
        _input, 166, _ctx)) {
      case 1: {
        enterOuterAlt(_localctx, 1);
        setState(871);
        literal();
        break;
      }

      case 2: {
        enterOuterAlt(_localctx, 2);
        setState(872);
        parameter();
        break;
      }

      case 3: {
        enterOuterAlt(_localctx, 3);
        setState(873);
        match(CypherParser::COUNT);
        setState(875);
        _errHandler->sync(this);

        _la = _input->LA(1);
        if (_la == CypherParser::SP) {
          setState(874);
          match(CypherParser::SP);
        }
        setState(877);
        match(CypherParser::T__5);
        setState(879);
        _errHandler->sync(this);

        _la = _input->LA(1);
        if (_la == CypherParser::SP) {
          setState(878);
          match(CypherParser::SP);
        }
        setState(881);
        match(CypherParser::T__4);
        setState(883);
        _errHandler->sync(this);

        _la = _input->LA(1);
        if (_la == CypherParser::SP) {
          setState(882);
          match(CypherParser::SP);
        }
        setState(885);
        match(CypherParser::T__6);
        break;
      }

      case 4: {
        enterOuterAlt(_localctx, 4);
        setState(886);
        listComprehension();
        break;
      }

      case 5: {
        enterOuterAlt(_localctx, 5);
        setState(887);
        match(CypherParser::FILTER);
        setState(889);
        _errHandler->sync(this);

        _la = _input->LA(1);
        if (_la == CypherParser::SP) {
          setState(888);
          match(CypherParser::SP);
        }
        setState(891);
        match(CypherParser::T__5);
        setState(893);
        _errHandler->sync(this);

        _la = _input->LA(1);
        if (_la == CypherParser::SP) {
          setState(892);
          match(CypherParser::SP);
        }
        setState(895);
        filterExpression();
        setState(897);
        _errHandler->sync(this);

        _la = _input->LA(1);
        if (_la == CypherParser::SP) {
          setState(896);
          match(CypherParser::SP);
        }
        setState(899);
        match(CypherParser::T__6);
        break;
      }

      case 6: {
        enterOuterAlt(_localctx, 6);
        setState(901);
        match(CypherParser::EXTRACT);
        setState(903);
        _errHandler->sync(this);

        _la = _input->LA(1);
        if (_la == CypherParser::SP) {
          setState(902);
          match(CypherParser::SP);
        }
        setState(905);
        match(CypherParser::T__5);
        setState(907);
        _errHandler->sync(this);

        _la = _input->LA(1);
        if (_la == CypherParser::SP) {
          setState(906);
          match(CypherParser::SP);
        }
        setState(909);
        filterExpression();
        setState(911);
        _errHandler->sync(this);

        switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(
            _input, 151, _ctx)) {
          case 1: {
            setState(910);
            match(CypherParser::SP);
            break;
          }
        }
        setState(918);
        _errHandler->sync(this);

        _la = _input->LA(1);
        if (_la == CypherParser::T__11 || _la == CypherParser::SP) {
          setState(914);
          _errHandler->sync(this);

          _la = _input->LA(1);
          if (_la == CypherParser::SP) {
            setState(913);
            match(CypherParser::SP);
          }
          setState(916);
          match(CypherParser::T__11);
          setState(917);
          expression();
        }
        setState(920);
        match(CypherParser::T__6);
        break;
      }

      case 7: {
        enterOuterAlt(_localctx, 7);
        setState(922);
        match(CypherParser::ALL);
        setState(924);
        _errHandler->sync(this);

        _la = _input->LA(1);
        if (_la == CypherParser::SP) {
          setState(923);
          match(CypherParser::SP);
        }
        setState(926);
        match(CypherParser::T__5);
        setState(928);
        _errHandler->sync(this);

        _la = _input->LA(1);
        if (_la == CypherParser::SP) {
          setState(927);
          match(CypherParser::SP);
        }
        setState(930);
        filterExpression();
        setState(932);
        _errHandler->sync(this);

        _la = _input->LA(1);
        if (_la == CypherParser::SP) {
          setState(931);
          match(CypherParser::SP);
        }
        setState(934);
        match(CypherParser::T__6);
        break;
      }

      case 8: {
        enterOuterAlt(_localctx, 8);
        setState(936);
        match(CypherParser::ANY);
        setState(938);
        _errHandler->sync(this);

        _la = _input->LA(1);
        if (_la == CypherParser::SP) {
          setState(937);
          match(CypherParser::SP);
        }
        setState(940);
        match(CypherParser::T__5);
        setState(942);
        _errHandler->sync(this);

        _la = _input->LA(1);
        if (_la == CypherParser::SP) {
          setState(941);
          match(CypherParser::SP);
        }
        setState(944);
        filterExpression();
        setState(946);
        _errHandler->sync(this);

        _la = _input->LA(1);
        if (_la == CypherParser::SP) {
          setState(945);
          match(CypherParser::SP);
        }
        setState(948);
        match(CypherParser::T__6);
        break;
      }

      case 9: {
        enterOuterAlt(_localctx, 9);
        setState(950);
        match(CypherParser::NONE);
        setState(952);
        _errHandler->sync(this);

        _la = _input->LA(1);
        if (_la == CypherParser::SP) {
          setState(951);
          match(CypherParser::SP);
        }
        setState(954);
        match(CypherParser::T__5);
        setState(956);
        _errHandler->sync(this);

        _la = _input->LA(1);
        if (_la == CypherParser::SP) {
          setState(955);
          match(CypherParser::SP);
        }
        setState(958);
        filterExpression();
        setState(960);
        _errHandler->sync(this);

        _la = _input->LA(1);
        if (_la == CypherParser::SP) {
          setState(959);
          match(CypherParser::SP);
        }
        setState(962);
        match(CypherParser::T__6);
        break;
      }

      case 10: {
        enterOuterAlt(_localctx, 10);
        setState(964);
        match(CypherParser::SINGLE);
        setState(966);
        _errHandler->sync(this);

        _la = _input->LA(1);
        if (_la == CypherParser::SP) {
          setState(965);
          match(CypherParser::SP);
        }
        setState(968);
        match(CypherParser::T__5);
        setState(970);
        _errHandler->sync(this);

        _la = _input->LA(1);
        if (_la == CypherParser::SP) {
          setState(969);
          match(CypherParser::SP);
        }
        setState(972);
        filterExpression();
        setState(974);
        _errHandler->sync(this);

        _la = _input->LA(1);
        if (_la == CypherParser::SP) {
          setState(973);
          match(CypherParser::SP);
        }
        setState(976);
        match(CypherParser::T__6);
        break;
      }

      case 11: {
        enterOuterAlt(_localctx, 11);
        setState(978);
        relationshipsPattern();
        break;
      }

      case 12: {
        enterOuterAlt(_localctx, 12);
        setState(979);
        parenthesizedExpression();
        break;
      }

      case 13: {
        enterOuterAlt(_localctx, 13);
        setState(980);
        functionInvocation();
        break;
      }

      case 14: {
        enterOuterAlt(_localctx, 14);
        setState(981);
        variable();
        break;
      }
    }

  } catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- LiteralContext
//------------------------------------------------------------------

CypherParser::LiteralContext::LiteralContext(ParserRuleContext *parent,
                                             size_t invokingState)
    : ParserRuleContext(parent, invokingState) {}

CypherParser::NumberLiteralContext *
CypherParser::LiteralContext::numberLiteral() {
  return getRuleContext<CypherParser::NumberLiteralContext>(0);
}

tree::TerminalNode *CypherParser::LiteralContext::StringLiteral() {
  return getToken(CypherParser::StringLiteral, 0);
}

CypherParser::BooleanLiteralContext *
CypherParser::LiteralContext::booleanLiteral() {
  return getRuleContext<CypherParser::BooleanLiteralContext>(0);
}

tree::TerminalNode *CypherParser::LiteralContext::CYPHERNULL() {
  return getToken(CypherParser::CYPHERNULL, 0);
}

CypherParser::MapLiteralContext *CypherParser::LiteralContext::mapLiteral() {
  return getRuleContext<CypherParser::MapLiteralContext>(0);
}

CypherParser::ListLiteralContext *CypherParser::LiteralContext::listLiteral() {
  return getRuleContext<CypherParser::ListLiteralContext>(0);
}

size_t CypherParser::LiteralContext::getRuleIndex() const {
  return CypherParser::RuleLiteral;
}

void CypherParser::LiteralContext::enterRule(
    tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<CypherListener *>(listener);
  if (parserListener != nullptr) parserListener->enterLiteral(this);
}

void CypherParser::LiteralContext::exitRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<CypherListener *>(listener);
  if (parserListener != nullptr) parserListener->exitLiteral(this);
}

antlrcpp::Any CypherParser::LiteralContext::accept(
    tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<CypherVisitor *>(visitor))
    return parserVisitor->visitLiteral(this);
  else
    return visitor->visitChildren(this);
}

CypherParser::LiteralContext *CypherParser::literal() {
  LiteralContext *_localctx =
      _tracker.createInstance<LiteralContext>(_ctx, getState());
  enterRule(_localctx, 110, CypherParser::RuleLiteral);

  auto onExit = finally([=] { exitRule(); });
  try {
    setState(990);
    _errHandler->sync(this);
    switch (_input->LA(1)) {
      case CypherParser::HexInteger:
      case CypherParser::DecimalInteger:
      case CypherParser::OctalInteger:
      case CypherParser::ExponentDecimalReal:
      case CypherParser::RegularDecimalReal: {
        enterOuterAlt(_localctx, 1);
        setState(984);
        numberLiteral();
        break;
      }

      case CypherParser::StringLiteral: {
        enterOuterAlt(_localctx, 2);
        setState(985);
        match(CypherParser::StringLiteral);
        break;
      }

      case CypherParser::TRUE:
      case CypherParser::FALSE: {
        enterOuterAlt(_localctx, 3);
        setState(986);
        booleanLiteral();
        break;
      }

      case CypherParser::CYPHERNULL: {
        enterOuterAlt(_localctx, 4);
        setState(987);
        match(CypherParser::CYPHERNULL);
        break;
      }

      case CypherParser::T__27: {
        enterOuterAlt(_localctx, 5);
        setState(988);
        mapLiteral();
        break;
      }

      case CypherParser::T__7: {
        enterOuterAlt(_localctx, 6);
        setState(989);
        listLiteral();
        break;
      }

      default:
        throw NoViableAltException(this);
    }

  } catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- BooleanLiteralContext
//------------------------------------------------------------------

CypherParser::BooleanLiteralContext::BooleanLiteralContext(
    ParserRuleContext *parent, size_t invokingState)
    : ParserRuleContext(parent, invokingState) {}

tree::TerminalNode *CypherParser::BooleanLiteralContext::TRUE() {
  return getToken(CypherParser::TRUE, 0);
}

tree::TerminalNode *CypherParser::BooleanLiteralContext::FALSE() {
  return getToken(CypherParser::FALSE, 0);
}

size_t CypherParser::BooleanLiteralContext::getRuleIndex() const {
  return CypherParser::RuleBooleanLiteral;
}

void CypherParser::BooleanLiteralContext::enterRule(
    tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<CypherListener *>(listener);
  if (parserListener != nullptr) parserListener->enterBooleanLiteral(this);
}

void CypherParser::BooleanLiteralContext::exitRule(
    tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<CypherListener *>(listener);
  if (parserListener != nullptr) parserListener->exitBooleanLiteral(this);
}

antlrcpp::Any CypherParser::BooleanLiteralContext::accept(
    tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<CypherVisitor *>(visitor))
    return parserVisitor->visitBooleanLiteral(this);
  else
    return visitor->visitChildren(this);
}

CypherParser::BooleanLiteralContext *CypherParser::booleanLiteral() {
  BooleanLiteralContext *_localctx =
      _tracker.createInstance<BooleanLiteralContext>(_ctx, getState());
  enterRule(_localctx, 112, CypherParser::RuleBooleanLiteral);
  size_t _la = 0;

  auto onExit = finally([=] { exitRule(); });
  try {
    enterOuterAlt(_localctx, 1);
    setState(992);
    _la = _input->LA(1);
    if (!(_la == CypherParser::TRUE

          || _la == CypherParser::FALSE)) {
      _errHandler->recoverInline(this);
    } else {
      _errHandler->reportMatch(this);
      consume();
    }

  } catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- ListLiteralContext
//------------------------------------------------------------------

CypherParser::ListLiteralContext::ListLiteralContext(ParserRuleContext *parent,
                                                     size_t invokingState)
    : ParserRuleContext(parent, invokingState) {}

std::vector<tree::TerminalNode *> CypherParser::ListLiteralContext::SP() {
  return getTokens(CypherParser::SP);
}

tree::TerminalNode *CypherParser::ListLiteralContext::SP(size_t i) {
  return getToken(CypherParser::SP, i);
}

std::vector<CypherParser::ExpressionContext *>
CypherParser::ListLiteralContext::expression() {
  return getRuleContexts<CypherParser::ExpressionContext>();
}

CypherParser::ExpressionContext *CypherParser::ListLiteralContext::expression(
    size_t i) {
  return getRuleContext<CypherParser::ExpressionContext>(i);
}

size_t CypherParser::ListLiteralContext::getRuleIndex() const {
  return CypherParser::RuleListLiteral;
}

void CypherParser::ListLiteralContext::enterRule(
    tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<CypherListener *>(listener);
  if (parserListener != nullptr) parserListener->enterListLiteral(this);
}

void CypherParser::ListLiteralContext::exitRule(
    tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<CypherListener *>(listener);
  if (parserListener != nullptr) parserListener->exitListLiteral(this);
}

antlrcpp::Any CypherParser::ListLiteralContext::accept(
    tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<CypherVisitor *>(visitor))
    return parserVisitor->visitListLiteral(this);
  else
    return visitor->visitChildren(this);
}

CypherParser::ListLiteralContext *CypherParser::listLiteral() {
  ListLiteralContext *_localctx =
      _tracker.createInstance<ListLiteralContext>(_ctx, getState());
  enterRule(_localctx, 114, CypherParser::RuleListLiteral);
  size_t _la = 0;

  auto onExit = finally([=] { exitRule(); });
  try {
    enterOuterAlt(_localctx, 1);
    setState(994);
    match(CypherParser::T__7);
    setState(996);
    _errHandler->sync(this);

    _la = _input->LA(1);
    if (_la == CypherParser::SP) {
      setState(995);
      match(CypherParser::SP);
    }
    setState(1015);
    _errHandler->sync(this);

    _la = _input->LA(1);
    if ((((_la & ~0x3fULL) == 0) &&
         ((1ULL << _la) &
          ((1ULL << CypherParser::T__5) | (1ULL << CypherParser::T__7) |
           (1ULL << CypherParser::T__13) | (1ULL << CypherParser::T__14) |
           (1ULL << CypherParser::T__27) | (1ULL << CypherParser::T__29) |
           (1ULL << CypherParser::StringLiteral) |
           (1ULL << CypherParser::HexInteger) |
           (1ULL << CypherParser::DecimalInteger) |
           (1ULL << CypherParser::OctalInteger) |
           (1ULL << CypherParser::HexLetter) |
           (1ULL << CypherParser::ExponentDecimalReal) |
           (1ULL << CypherParser::RegularDecimalReal))) != 0) ||
        ((((_la - 64) & ~0x3fULL) == 0) &&
         ((1ULL << (_la - 64)) &
          ((1ULL << (CypherParser::UNION - 64)) |
           (1ULL << (CypherParser::ALL - 64)) |
           (1ULL << (CypherParser::OPTIONAL - 64)) |
           (1ULL << (CypherParser::MATCH - 64)) |
           (1ULL << (CypherParser::UNWIND - 64)) |
           (1ULL << (CypherParser::AS - 64)) |
           (1ULL << (CypherParser::MERGE - 64)) |
           (1ULL << (CypherParser::ON - 64)) |
           (1ULL << (CypherParser::CREATE - 64)) |
           (1ULL << (CypherParser::SET - 64)) |
           (1ULL << (CypherParser::DETACH - 64)) |
           (1ULL << (CypherParser::DELETE - 64)) |
           (1ULL << (CypherParser::REMOVE - 64)) |
           (1ULL << (CypherParser::WITH - 64)) |
           (1ULL << (CypherParser::DISTINCT - 64)) |
           (1ULL << (CypherParser::RETURN - 64)) |
           (1ULL << (CypherParser::ORDER - 64)) |
           (1ULL << (CypherParser::BY - 64)) |
           (1ULL << (CypherParser::L_SKIP - 64)) |
           (1ULL << (CypherParser::LIMIT - 64)) |
           (1ULL << (CypherParser::ASCENDING - 64)) |
           (1ULL << (CypherParser::ASC - 64)) |
           (1ULL << (CypherParser::DESCENDING - 64)) |
           (1ULL << (CypherParser::DESC - 64)) |
           (1ULL << (CypherParser::WHERE - 64)) |
           (1ULL << (CypherParser::OR - 64)) |
           (1ULL << (CypherParser::XOR - 64)) |
           (1ULL << (CypherParser::AND - 64)) |
           (1ULL << (CypherParser::NOT - 64)) |
           (1ULL << (CypherParser::IN - 64)) |
           (1ULL << (CypherParser::STARTS - 64)) |
           (1ULL << (CypherParser::ENDS - 64)) |
           (1ULL << (CypherParser::CONTAINS - 64)) |
           (1ULL << (CypherParser::IS - 64)) |
           (1ULL << (CypherParser::CYPHERNULL - 64)) |
           (1ULL << (CypherParser::COUNT - 64)) |
           (1ULL << (CypherParser::FILTER - 64)) |
           (1ULL << (CypherParser::EXTRACT - 64)) |
           (1ULL << (CypherParser::ANY - 64)) |
           (1ULL << (CypherParser::NONE - 64)) |
           (1ULL << (CypherParser::SINGLE - 64)) |
           (1ULL << (CypherParser::TRUE - 64)) |
           (1ULL << (CypherParser::FALSE - 64)) |
           (1ULL << (CypherParser::UnescapedSymbolicName - 64)) |
           (1ULL << (CypherParser::EscapedSymbolicName - 64)))) != 0)) {
      setState(998);
      expression();
      setState(1000);
      _errHandler->sync(this);

      _la = _input->LA(1);
      if (_la == CypherParser::SP) {
        setState(999);
        match(CypherParser::SP);
      }
      setState(1012);
      _errHandler->sync(this);
      _la = _input->LA(1);
      while (_la == CypherParser::T__1) {
        setState(1002);
        match(CypherParser::T__1);
        setState(1004);
        _errHandler->sync(this);

        _la = _input->LA(1);
        if (_la == CypherParser::SP) {
          setState(1003);
          match(CypherParser::SP);
        }
        setState(1006);
        expression();
        setState(1008);
        _errHandler->sync(this);

        _la = _input->LA(1);
        if (_la == CypherParser::SP) {
          setState(1007);
          match(CypherParser::SP);
        }
        setState(1014);
        _errHandler->sync(this);
        _la = _input->LA(1);
      }
    }
    setState(1017);
    match(CypherParser::T__9);

  } catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- PartialComparisonExpressionContext
//------------------------------------------------------------------

CypherParser::PartialComparisonExpressionContext::
    PartialComparisonExpressionContext(ParserRuleContext *parent,
                                       size_t invokingState)
    : ParserRuleContext(parent, invokingState) {}

CypherParser::Expression7Context *
CypherParser::PartialComparisonExpressionContext::expression7() {
  return getRuleContext<CypherParser::Expression7Context>(0);
}

tree::TerminalNode *CypherParser::PartialComparisonExpressionContext::SP() {
  return getToken(CypherParser::SP, 0);
}

size_t CypherParser::PartialComparisonExpressionContext::getRuleIndex() const {
  return CypherParser::RulePartialComparisonExpression;
}

void CypherParser::PartialComparisonExpressionContext::enterRule(
    tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<CypherListener *>(listener);
  if (parserListener != nullptr)
    parserListener->enterPartialComparisonExpression(this);
}

void CypherParser::PartialComparisonExpressionContext::exitRule(
    tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<CypherListener *>(listener);
  if (parserListener != nullptr)
    parserListener->exitPartialComparisonExpression(this);
}

antlrcpp::Any CypherParser::PartialComparisonExpressionContext::accept(
    tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<CypherVisitor *>(visitor))
    return parserVisitor->visitPartialComparisonExpression(this);
  else
    return visitor->visitChildren(this);
}

CypherParser::PartialComparisonExpressionContext *
CypherParser::partialComparisonExpression() {
  PartialComparisonExpressionContext *_localctx =
      _tracker.createInstance<PartialComparisonExpressionContext>(_ctx,
                                                                  getState());
  enterRule(_localctx, 116, CypherParser::RulePartialComparisonExpression);
  size_t _la = 0;

  auto onExit = finally([=] { exitRule(); });
  try {
    setState(1054);
    _errHandler->sync(this);
    switch (_input->LA(1)) {
      case CypherParser::T__2: {
        enterOuterAlt(_localctx, 1);
        setState(1019);
        match(CypherParser::T__2);
        setState(1021);
        _errHandler->sync(this);

        _la = _input->LA(1);
        if (_la == CypherParser::SP) {
          setState(1020);
          match(CypherParser::SP);
        }
        setState(1023);
        expression7();
        break;
      }

      case CypherParser::T__19: {
        enterOuterAlt(_localctx, 2);
        setState(1024);
        match(CypherParser::T__19);
        setState(1026);
        _errHandler->sync(this);

        _la = _input->LA(1);
        if (_la == CypherParser::SP) {
          setState(1025);
          match(CypherParser::SP);
        }
        setState(1028);
        expression7();
        break;
      }

      case CypherParser::T__20: {
        enterOuterAlt(_localctx, 3);
        setState(1029);
        match(CypherParser::T__20);
        setState(1031);
        _errHandler->sync(this);

        _la = _input->LA(1);
        if (_la == CypherParser::SP) {
          setState(1030);
          match(CypherParser::SP);
        }
        setState(1033);
        expression7();
        break;
      }

      case CypherParser::T__21: {
        enterOuterAlt(_localctx, 4);
        setState(1034);
        match(CypherParser::T__21);
        setState(1036);
        _errHandler->sync(this);

        _la = _input->LA(1);
        if (_la == CypherParser::SP) {
          setState(1035);
          match(CypherParser::SP);
        }
        setState(1038);
        expression7();
        break;
      }

      case CypherParser::T__22: {
        enterOuterAlt(_localctx, 5);
        setState(1039);
        match(CypherParser::T__22);
        setState(1041);
        _errHandler->sync(this);

        _la = _input->LA(1);
        if (_la == CypherParser::SP) {
          setState(1040);
          match(CypherParser::SP);
        }
        setState(1043);
        expression7();
        break;
      }

      case CypherParser::T__23: {
        enterOuterAlt(_localctx, 6);
        setState(1044);
        match(CypherParser::T__23);
        setState(1046);
        _errHandler->sync(this);

        _la = _input->LA(1);
        if (_la == CypherParser::SP) {
          setState(1045);
          match(CypherParser::SP);
        }
        setState(1048);
        expression7();
        break;
      }

      case CypherParser::T__24: {
        enterOuterAlt(_localctx, 7);
        setState(1049);
        match(CypherParser::T__24);
        setState(1051);
        _errHandler->sync(this);

        _la = _input->LA(1);
        if (_la == CypherParser::SP) {
          setState(1050);
          match(CypherParser::SP);
        }
        setState(1053);
        expression7();
        break;
      }

      default:
        throw NoViableAltException(this);
    }

  } catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- ParenthesizedExpressionContext
//------------------------------------------------------------------

CypherParser::ParenthesizedExpressionContext::ParenthesizedExpressionContext(
    ParserRuleContext *parent, size_t invokingState)
    : ParserRuleContext(parent, invokingState) {}

CypherParser::ExpressionContext *
CypherParser::ParenthesizedExpressionContext::expression() {
  return getRuleContext<CypherParser::ExpressionContext>(0);
}

std::vector<tree::TerminalNode *>
CypherParser::ParenthesizedExpressionContext::SP() {
  return getTokens(CypherParser::SP);
}

tree::TerminalNode *CypherParser::ParenthesizedExpressionContext::SP(size_t i) {
  return getToken(CypherParser::SP, i);
}

size_t CypherParser::ParenthesizedExpressionContext::getRuleIndex() const {
  return CypherParser::RuleParenthesizedExpression;
}

void CypherParser::ParenthesizedExpressionContext::enterRule(
    tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<CypherListener *>(listener);
  if (parserListener != nullptr)
    parserListener->enterParenthesizedExpression(this);
}

void CypherParser::ParenthesizedExpressionContext::exitRule(
    tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<CypherListener *>(listener);
  if (parserListener != nullptr)
    parserListener->exitParenthesizedExpression(this);
}

antlrcpp::Any CypherParser::ParenthesizedExpressionContext::accept(
    tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<CypherVisitor *>(visitor))
    return parserVisitor->visitParenthesizedExpression(this);
  else
    return visitor->visitChildren(this);
}

CypherParser::ParenthesizedExpressionContext *
CypherParser::parenthesizedExpression() {
  ParenthesizedExpressionContext *_localctx =
      _tracker.createInstance<ParenthesizedExpressionContext>(_ctx, getState());
  enterRule(_localctx, 118, CypherParser::RuleParenthesizedExpression);
  size_t _la = 0;

  auto onExit = finally([=] { exitRule(); });
  try {
    enterOuterAlt(_localctx, 1);
    setState(1056);
    match(CypherParser::T__5);
    setState(1058);
    _errHandler->sync(this);

    _la = _input->LA(1);
    if (_la == CypherParser::SP) {
      setState(1057);
      match(CypherParser::SP);
    }
    setState(1060);
    expression();
    setState(1062);
    _errHandler->sync(this);

    _la = _input->LA(1);
    if (_la == CypherParser::SP) {
      setState(1061);
      match(CypherParser::SP);
    }
    setState(1064);
    match(CypherParser::T__6);

  } catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- RelationshipsPatternContext
//------------------------------------------------------------------

CypherParser::RelationshipsPatternContext::RelationshipsPatternContext(
    ParserRuleContext *parent, size_t invokingState)
    : ParserRuleContext(parent, invokingState) {}

CypherParser::NodePatternContext *
CypherParser::RelationshipsPatternContext::nodePattern() {
  return getRuleContext<CypherParser::NodePatternContext>(0);
}

std::vector<CypherParser::PatternElementChainContext *>
CypherParser::RelationshipsPatternContext::patternElementChain() {
  return getRuleContexts<CypherParser::PatternElementChainContext>();
}

CypherParser::PatternElementChainContext *
CypherParser::RelationshipsPatternContext::patternElementChain(size_t i) {
  return getRuleContext<CypherParser::PatternElementChainContext>(i);
}

std::vector<tree::TerminalNode *>
CypherParser::RelationshipsPatternContext::SP() {
  return getTokens(CypherParser::SP);
}

tree::TerminalNode *CypherParser::RelationshipsPatternContext::SP(size_t i) {
  return getToken(CypherParser::SP, i);
}

size_t CypherParser::RelationshipsPatternContext::getRuleIndex() const {
  return CypherParser::RuleRelationshipsPattern;
}

void CypherParser::RelationshipsPatternContext::enterRule(
    tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<CypherListener *>(listener);
  if (parserListener != nullptr)
    parserListener->enterRelationshipsPattern(this);
}

void CypherParser::RelationshipsPatternContext::exitRule(
    tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<CypherListener *>(listener);
  if (parserListener != nullptr) parserListener->exitRelationshipsPattern(this);
}

antlrcpp::Any CypherParser::RelationshipsPatternContext::accept(
    tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<CypherVisitor *>(visitor))
    return parserVisitor->visitRelationshipsPattern(this);
  else
    return visitor->visitChildren(this);
}

CypherParser::RelationshipsPatternContext *
CypherParser::relationshipsPattern() {
  RelationshipsPatternContext *_localctx =
      _tracker.createInstance<RelationshipsPatternContext>(_ctx, getState());
  enterRule(_localctx, 120, CypherParser::RuleRelationshipsPattern);
  size_t _la = 0;

  auto onExit = finally([=] { exitRule(); });
  try {
    size_t alt;
    enterOuterAlt(_localctx, 1);
    setState(1066);
    nodePattern();
    setState(1071);
    _errHandler->sync(this);
    alt = 1;
    do {
      switch (alt) {
        case 1: {
          setState(1068);
          _errHandler->sync(this);

          _la = _input->LA(1);
          if (_la == CypherParser::SP) {
            setState(1067);
            match(CypherParser::SP);
          }
          setState(1070);
          patternElementChain();
          break;
        }

        default:
          throw NoViableAltException(this);
      }
      setState(1073);
      _errHandler->sync(this);
      alt = getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(
          _input, 185, _ctx);
    } while (alt != 2 && alt != atn::ATN::INVALID_ALT_NUMBER);

  } catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- FilterExpressionContext
//------------------------------------------------------------------

CypherParser::FilterExpressionContext::FilterExpressionContext(
    ParserRuleContext *parent, size_t invokingState)
    : ParserRuleContext(parent, invokingState) {}

CypherParser::IdInCollContext *
CypherParser::FilterExpressionContext::idInColl() {
  return getRuleContext<CypherParser::IdInCollContext>(0);
}

CypherParser::WhereContext *CypherParser::FilterExpressionContext::where() {
  return getRuleContext<CypherParser::WhereContext>(0);
}

tree::TerminalNode *CypherParser::FilterExpressionContext::SP() {
  return getToken(CypherParser::SP, 0);
}

size_t CypherParser::FilterExpressionContext::getRuleIndex() const {
  return CypherParser::RuleFilterExpression;
}

void CypherParser::FilterExpressionContext::enterRule(
    tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<CypherListener *>(listener);
  if (parserListener != nullptr) parserListener->enterFilterExpression(this);
}

void CypherParser::FilterExpressionContext::exitRule(
    tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<CypherListener *>(listener);
  if (parserListener != nullptr) parserListener->exitFilterExpression(this);
}

antlrcpp::Any CypherParser::FilterExpressionContext::accept(
    tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<CypherVisitor *>(visitor))
    return parserVisitor->visitFilterExpression(this);
  else
    return visitor->visitChildren(this);
}

CypherParser::FilterExpressionContext *CypherParser::filterExpression() {
  FilterExpressionContext *_localctx =
      _tracker.createInstance<FilterExpressionContext>(_ctx, getState());
  enterRule(_localctx, 122, CypherParser::RuleFilterExpression);
  size_t _la = 0;

  auto onExit = finally([=] { exitRule(); });
  try {
    enterOuterAlt(_localctx, 1);
    setState(1075);
    idInColl();
    setState(1080);
    _errHandler->sync(this);

    switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(
        _input, 187, _ctx)) {
      case 1: {
        setState(1077);
        _errHandler->sync(this);

        _la = _input->LA(1);
        if (_la == CypherParser::SP) {
          setState(1076);
          match(CypherParser::SP);
        }
        setState(1079);
        where();
        break;
      }
    }

  } catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- IdInCollContext
//------------------------------------------------------------------

CypherParser::IdInCollContext::IdInCollContext(ParserRuleContext *parent,
                                               size_t invokingState)
    : ParserRuleContext(parent, invokingState) {}

CypherParser::VariableContext *CypherParser::IdInCollContext::variable() {
  return getRuleContext<CypherParser::VariableContext>(0);
}

std::vector<tree::TerminalNode *> CypherParser::IdInCollContext::SP() {
  return getTokens(CypherParser::SP);
}

tree::TerminalNode *CypherParser::IdInCollContext::SP(size_t i) {
  return getToken(CypherParser::SP, i);
}

tree::TerminalNode *CypherParser::IdInCollContext::IN() {
  return getToken(CypherParser::IN, 0);
}

CypherParser::ExpressionContext *CypherParser::IdInCollContext::expression() {
  return getRuleContext<CypherParser::ExpressionContext>(0);
}

size_t CypherParser::IdInCollContext::getRuleIndex() const {
  return CypherParser::RuleIdInColl;
}

void CypherParser::IdInCollContext::enterRule(
    tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<CypherListener *>(listener);
  if (parserListener != nullptr) parserListener->enterIdInColl(this);
}

void CypherParser::IdInCollContext::exitRule(
    tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<CypherListener *>(listener);
  if (parserListener != nullptr) parserListener->exitIdInColl(this);
}

antlrcpp::Any CypherParser::IdInCollContext::accept(
    tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<CypherVisitor *>(visitor))
    return parserVisitor->visitIdInColl(this);
  else
    return visitor->visitChildren(this);
}

CypherParser::IdInCollContext *CypherParser::idInColl() {
  IdInCollContext *_localctx =
      _tracker.createInstance<IdInCollContext>(_ctx, getState());
  enterRule(_localctx, 124, CypherParser::RuleIdInColl);

  auto onExit = finally([=] { exitRule(); });
  try {
    enterOuterAlt(_localctx, 1);
    setState(1082);
    variable();
    setState(1083);
    match(CypherParser::SP);
    setState(1084);
    match(CypherParser::IN);
    setState(1085);
    match(CypherParser::SP);
    setState(1086);
    expression();

  } catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- FunctionInvocationContext
//------------------------------------------------------------------

CypherParser::FunctionInvocationContext::FunctionInvocationContext(
    ParserRuleContext *parent, size_t invokingState)
    : ParserRuleContext(parent, invokingState) {}

CypherParser::FunctionNameContext *
CypherParser::FunctionInvocationContext::functionName() {
  return getRuleContext<CypherParser::FunctionNameContext>(0);
}

std::vector<tree::TerminalNode *>
CypherParser::FunctionInvocationContext::SP() {
  return getTokens(CypherParser::SP);
}

tree::TerminalNode *CypherParser::FunctionInvocationContext::SP(size_t i) {
  return getToken(CypherParser::SP, i);
}

tree::TerminalNode *CypherParser::FunctionInvocationContext::DISTINCT() {
  return getToken(CypherParser::DISTINCT, 0);
}

std::vector<CypherParser::ExpressionContext *>
CypherParser::FunctionInvocationContext::expression() {
  return getRuleContexts<CypherParser::ExpressionContext>();
}

CypherParser::ExpressionContext *
CypherParser::FunctionInvocationContext::expression(size_t i) {
  return getRuleContext<CypherParser::ExpressionContext>(i);
}

size_t CypherParser::FunctionInvocationContext::getRuleIndex() const {
  return CypherParser::RuleFunctionInvocation;
}

void CypherParser::FunctionInvocationContext::enterRule(
    tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<CypherListener *>(listener);
  if (parserListener != nullptr) parserListener->enterFunctionInvocation(this);
}

void CypherParser::FunctionInvocationContext::exitRule(
    tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<CypherListener *>(listener);
  if (parserListener != nullptr) parserListener->exitFunctionInvocation(this);
}

antlrcpp::Any CypherParser::FunctionInvocationContext::accept(
    tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<CypherVisitor *>(visitor))
    return parserVisitor->visitFunctionInvocation(this);
  else
    return visitor->visitChildren(this);
}

CypherParser::FunctionInvocationContext *CypherParser::functionInvocation() {
  FunctionInvocationContext *_localctx =
      _tracker.createInstance<FunctionInvocationContext>(_ctx, getState());
  enterRule(_localctx, 126, CypherParser::RuleFunctionInvocation);
  size_t _la = 0;

  auto onExit = finally([=] { exitRule(); });
  try {
    enterOuterAlt(_localctx, 1);
    setState(1088);
    functionName();
    setState(1090);
    _errHandler->sync(this);

    _la = _input->LA(1);
    if (_la == CypherParser::SP) {
      setState(1089);
      match(CypherParser::SP);
    }
    setState(1092);
    match(CypherParser::T__5);
    setState(1094);
    _errHandler->sync(this);

    _la = _input->LA(1);
    if (_la == CypherParser::SP) {
      setState(1093);
      match(CypherParser::SP);
    }
    setState(1100);
    _errHandler->sync(this);

    switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(
        _input, 191, _ctx)) {
      case 1: {
        setState(1096);
        match(CypherParser::DISTINCT);
        setState(1098);
        _errHandler->sync(this);

        _la = _input->LA(1);
        if (_la == CypherParser::SP) {
          setState(1097);
          match(CypherParser::SP);
        }
        break;
      }
    }
    setState(1119);
    _errHandler->sync(this);

    _la = _input->LA(1);
    if ((((_la & ~0x3fULL) == 0) &&
         ((1ULL << _la) &
          ((1ULL << CypherParser::T__5) | (1ULL << CypherParser::T__7) |
           (1ULL << CypherParser::T__13) | (1ULL << CypherParser::T__14) |
           (1ULL << CypherParser::T__27) | (1ULL << CypherParser::T__29) |
           (1ULL << CypherParser::StringLiteral) |
           (1ULL << CypherParser::HexInteger) |
           (1ULL << CypherParser::DecimalInteger) |
           (1ULL << CypherParser::OctalInteger) |
           (1ULL << CypherParser::HexLetter) |
           (1ULL << CypherParser::ExponentDecimalReal) |
           (1ULL << CypherParser::RegularDecimalReal))) != 0) ||
        ((((_la - 64) & ~0x3fULL) == 0) &&
         ((1ULL << (_la - 64)) &
          ((1ULL << (CypherParser::UNION - 64)) |
           (1ULL << (CypherParser::ALL - 64)) |
           (1ULL << (CypherParser::OPTIONAL - 64)) |
           (1ULL << (CypherParser::MATCH - 64)) |
           (1ULL << (CypherParser::UNWIND - 64)) |
           (1ULL << (CypherParser::AS - 64)) |
           (1ULL << (CypherParser::MERGE - 64)) |
           (1ULL << (CypherParser::ON - 64)) |
           (1ULL << (CypherParser::CREATE - 64)) |
           (1ULL << (CypherParser::SET - 64)) |
           (1ULL << (CypherParser::DETACH - 64)) |
           (1ULL << (CypherParser::DELETE - 64)) |
           (1ULL << (CypherParser::REMOVE - 64)) |
           (1ULL << (CypherParser::WITH - 64)) |
           (1ULL << (CypherParser::DISTINCT - 64)) |
           (1ULL << (CypherParser::RETURN - 64)) |
           (1ULL << (CypherParser::ORDER - 64)) |
           (1ULL << (CypherParser::BY - 64)) |
           (1ULL << (CypherParser::L_SKIP - 64)) |
           (1ULL << (CypherParser::LIMIT - 64)) |
           (1ULL << (CypherParser::ASCENDING - 64)) |
           (1ULL << (CypherParser::ASC - 64)) |
           (1ULL << (CypherParser::DESCENDING - 64)) |
           (1ULL << (CypherParser::DESC - 64)) |
           (1ULL << (CypherParser::WHERE - 64)) |
           (1ULL << (CypherParser::OR - 64)) |
           (1ULL << (CypherParser::XOR - 64)) |
           (1ULL << (CypherParser::AND - 64)) |
           (1ULL << (CypherParser::NOT - 64)) |
           (1ULL << (CypherParser::IN - 64)) |
           (1ULL << (CypherParser::STARTS - 64)) |
           (1ULL << (CypherParser::ENDS - 64)) |
           (1ULL << (CypherParser::CONTAINS - 64)) |
           (1ULL << (CypherParser::IS - 64)) |
           (1ULL << (CypherParser::CYPHERNULL - 64)) |
           (1ULL << (CypherParser::COUNT - 64)) |
           (1ULL << (CypherParser::FILTER - 64)) |
           (1ULL << (CypherParser::EXTRACT - 64)) |
           (1ULL << (CypherParser::ANY - 64)) |
           (1ULL << (CypherParser::NONE - 64)) |
           (1ULL << (CypherParser::SINGLE - 64)) |
           (1ULL << (CypherParser::TRUE - 64)) |
           (1ULL << (CypherParser::FALSE - 64)) |
           (1ULL << (CypherParser::UnescapedSymbolicName - 64)) |
           (1ULL << (CypherParser::EscapedSymbolicName - 64)))) != 0)) {
      setState(1102);
      expression();
      setState(1104);
      _errHandler->sync(this);

      _la = _input->LA(1);
      if (_la == CypherParser::SP) {
        setState(1103);
        match(CypherParser::SP);
      }
      setState(1116);
      _errHandler->sync(this);
      _la = _input->LA(1);
      while (_la == CypherParser::T__1) {
        setState(1106);
        match(CypherParser::T__1);
        setState(1108);
        _errHandler->sync(this);

        _la = _input->LA(1);
        if (_la == CypherParser::SP) {
          setState(1107);
          match(CypherParser::SP);
        }
        setState(1110);
        expression();
        setState(1112);
        _errHandler->sync(this);

        _la = _input->LA(1);
        if (_la == CypherParser::SP) {
          setState(1111);
          match(CypherParser::SP);
        }
        setState(1118);
        _errHandler->sync(this);
        _la = _input->LA(1);
      }
    }
    setState(1121);
    match(CypherParser::T__6);

  } catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- FunctionNameContext
//------------------------------------------------------------------

CypherParser::FunctionNameContext::FunctionNameContext(
    ParserRuleContext *parent, size_t invokingState)
    : ParserRuleContext(parent, invokingState) {}

tree::TerminalNode *CypherParser::FunctionNameContext::UnescapedSymbolicName() {
  return getToken(CypherParser::UnescapedSymbolicName, 0);
}

tree::TerminalNode *CypherParser::FunctionNameContext::EscapedSymbolicName() {
  return getToken(CypherParser::EscapedSymbolicName, 0);
}

tree::TerminalNode *CypherParser::FunctionNameContext::COUNT() {
  return getToken(CypherParser::COUNT, 0);
}

size_t CypherParser::FunctionNameContext::getRuleIndex() const {
  return CypherParser::RuleFunctionName;
}

void CypherParser::FunctionNameContext::enterRule(
    tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<CypherListener *>(listener);
  if (parserListener != nullptr) parserListener->enterFunctionName(this);
}

void CypherParser::FunctionNameContext::exitRule(
    tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<CypherListener *>(listener);
  if (parserListener != nullptr) parserListener->exitFunctionName(this);
}

antlrcpp::Any CypherParser::FunctionNameContext::accept(
    tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<CypherVisitor *>(visitor))
    return parserVisitor->visitFunctionName(this);
  else
    return visitor->visitChildren(this);
}

CypherParser::FunctionNameContext *CypherParser::functionName() {
  FunctionNameContext *_localctx =
      _tracker.createInstance<FunctionNameContext>(_ctx, getState());
  enterRule(_localctx, 128, CypherParser::RuleFunctionName);
  size_t _la = 0;

  auto onExit = finally([=] { exitRule(); });
  try {
    enterOuterAlt(_localctx, 1);
    setState(1123);
    _la = _input->LA(1);
    if (!(((((_la - 99) & ~0x3fULL) == 0) &&
           ((1ULL << (_la - 99)) &
            ((1ULL << (CypherParser::COUNT - 99)) |
             (1ULL << (CypherParser::UnescapedSymbolicName - 99)) |
             (1ULL << (CypherParser::EscapedSymbolicName - 99)))) != 0))) {
      _errHandler->recoverInline(this);
    } else {
      _errHandler->reportMatch(this);
      consume();
    }

  } catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- ListComprehensionContext
//------------------------------------------------------------------

CypherParser::ListComprehensionContext::ListComprehensionContext(
    ParserRuleContext *parent, size_t invokingState)
    : ParserRuleContext(parent, invokingState) {}

CypherParser::FilterExpressionContext *
CypherParser::ListComprehensionContext::filterExpression() {
  return getRuleContext<CypherParser::FilterExpressionContext>(0);
}

std::vector<tree::TerminalNode *> CypherParser::ListComprehensionContext::SP() {
  return getTokens(CypherParser::SP);
}

tree::TerminalNode *CypherParser::ListComprehensionContext::SP(size_t i) {
  return getToken(CypherParser::SP, i);
}

CypherParser::ExpressionContext *
CypherParser::ListComprehensionContext::expression() {
  return getRuleContext<CypherParser::ExpressionContext>(0);
}

size_t CypherParser::ListComprehensionContext::getRuleIndex() const {
  return CypherParser::RuleListComprehension;
}

void CypherParser::ListComprehensionContext::enterRule(
    tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<CypherListener *>(listener);
  if (parserListener != nullptr) parserListener->enterListComprehension(this);
}

void CypherParser::ListComprehensionContext::exitRule(
    tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<CypherListener *>(listener);
  if (parserListener != nullptr) parserListener->exitListComprehension(this);
}

antlrcpp::Any CypherParser::ListComprehensionContext::accept(
    tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<CypherVisitor *>(visitor))
    return parserVisitor->visitListComprehension(this);
  else
    return visitor->visitChildren(this);
}

CypherParser::ListComprehensionContext *CypherParser::listComprehension() {
  ListComprehensionContext *_localctx =
      _tracker.createInstance<ListComprehensionContext>(_ctx, getState());
  enterRule(_localctx, 130, CypherParser::RuleListComprehension);
  size_t _la = 0;

  auto onExit = finally([=] { exitRule(); });
  try {
    enterOuterAlt(_localctx, 1);
    setState(1125);
    match(CypherParser::T__7);
    setState(1127);
    _errHandler->sync(this);

    _la = _input->LA(1);
    if (_la == CypherParser::SP) {
      setState(1126);
      match(CypherParser::SP);
    }
    setState(1129);
    filterExpression();
    setState(1138);
    _errHandler->sync(this);

    switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(
        _input, 200, _ctx)) {
      case 1: {
        setState(1131);
        _errHandler->sync(this);

        _la = _input->LA(1);
        if (_la == CypherParser::SP) {
          setState(1130);
          match(CypherParser::SP);
        }
        setState(1133);
        match(CypherParser::T__11);
        setState(1135);
        _errHandler->sync(this);

        _la = _input->LA(1);
        if (_la == CypherParser::SP) {
          setState(1134);
          match(CypherParser::SP);
        }
        setState(1137);
        expression();
        break;
      }
    }
    setState(1141);
    _errHandler->sync(this);

    _la = _input->LA(1);
    if (_la == CypherParser::SP) {
      setState(1140);
      match(CypherParser::SP);
    }
    setState(1143);
    match(CypherParser::T__9);

  } catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- PropertyLookupContext
//------------------------------------------------------------------

CypherParser::PropertyLookupContext::PropertyLookupContext(
    ParserRuleContext *parent, size_t invokingState)
    : ParserRuleContext(parent, invokingState) {}

CypherParser::PropertyKeyNameContext *
CypherParser::PropertyLookupContext::propertyKeyName() {
  return getRuleContext<CypherParser::PropertyKeyNameContext>(0);
}

std::vector<tree::TerminalNode *> CypherParser::PropertyLookupContext::SP() {
  return getTokens(CypherParser::SP);
}

tree::TerminalNode *CypherParser::PropertyLookupContext::SP(size_t i) {
  return getToken(CypherParser::SP, i);
}

size_t CypherParser::PropertyLookupContext::getRuleIndex() const {
  return CypherParser::RulePropertyLookup;
}

void CypherParser::PropertyLookupContext::enterRule(
    tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<CypherListener *>(listener);
  if (parserListener != nullptr) parserListener->enterPropertyLookup(this);
}

void CypherParser::PropertyLookupContext::exitRule(
    tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<CypherListener *>(listener);
  if (parserListener != nullptr) parserListener->exitPropertyLookup(this);
}

antlrcpp::Any CypherParser::PropertyLookupContext::accept(
    tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<CypherVisitor *>(visitor))
    return parserVisitor->visitPropertyLookup(this);
  else
    return visitor->visitChildren(this);
}

CypherParser::PropertyLookupContext *CypherParser::propertyLookup() {
  PropertyLookupContext *_localctx =
      _tracker.createInstance<PropertyLookupContext>(_ctx, getState());
  enterRule(_localctx, 132, CypherParser::RulePropertyLookup);
  size_t _la = 0;

  auto onExit = finally([=] { exitRule(); });
  try {
    enterOuterAlt(_localctx, 1);
    setState(1146);
    _errHandler->sync(this);

    _la = _input->LA(1);
    if (_la == CypherParser::SP) {
      setState(1145);
      match(CypherParser::SP);
    }
    setState(1148);
    match(CypherParser::T__25);
    setState(1150);
    _errHandler->sync(this);

    _la = _input->LA(1);
    if (_la == CypherParser::SP) {
      setState(1149);
      match(CypherParser::SP);
    }
    setState(1156);
    _errHandler->sync(this);
    switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(
        _input, 204, _ctx)) {
      case 1: {
        setState(1152);
        propertyKeyName();
        setState(1153);
        _la = _input->LA(1);
        if (!(_la == CypherParser::T__8

              || _la == CypherParser::T__26)) {
          _errHandler->recoverInline(this);
        } else {
          _errHandler->reportMatch(this);
          consume();
        }
        break;
      }

      case 2: {
        setState(1155);
        propertyKeyName();
        break;
      }
    }

  } catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- VariableContext
//------------------------------------------------------------------

CypherParser::VariableContext::VariableContext(ParserRuleContext *parent,
                                               size_t invokingState)
    : ParserRuleContext(parent, invokingState) {}

CypherParser::SymbolicNameContext *
CypherParser::VariableContext::symbolicName() {
  return getRuleContext<CypherParser::SymbolicNameContext>(0);
}

size_t CypherParser::VariableContext::getRuleIndex() const {
  return CypherParser::RuleVariable;
}

void CypherParser::VariableContext::enterRule(
    tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<CypherListener *>(listener);
  if (parserListener != nullptr) parserListener->enterVariable(this);
}

void CypherParser::VariableContext::exitRule(
    tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<CypherListener *>(listener);
  if (parserListener != nullptr) parserListener->exitVariable(this);
}

antlrcpp::Any CypherParser::VariableContext::accept(
    tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<CypherVisitor *>(visitor))
    return parserVisitor->visitVariable(this);
  else
    return visitor->visitChildren(this);
}

CypherParser::VariableContext *CypherParser::variable() {
  VariableContext *_localctx =
      _tracker.createInstance<VariableContext>(_ctx, getState());
  enterRule(_localctx, 134, CypherParser::RuleVariable);

  auto onExit = finally([=] { exitRule(); });
  try {
    enterOuterAlt(_localctx, 1);
    setState(1158);
    symbolicName();

  } catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- NumberLiteralContext
//------------------------------------------------------------------

CypherParser::NumberLiteralContext::NumberLiteralContext(
    ParserRuleContext *parent, size_t invokingState)
    : ParserRuleContext(parent, invokingState) {}

CypherParser::DoubleLiteralContext *
CypherParser::NumberLiteralContext::doubleLiteral() {
  return getRuleContext<CypherParser::DoubleLiteralContext>(0);
}

CypherParser::IntegerLiteralContext *
CypherParser::NumberLiteralContext::integerLiteral() {
  return getRuleContext<CypherParser::IntegerLiteralContext>(0);
}

size_t CypherParser::NumberLiteralContext::getRuleIndex() const {
  return CypherParser::RuleNumberLiteral;
}

void CypherParser::NumberLiteralContext::enterRule(
    tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<CypherListener *>(listener);
  if (parserListener != nullptr) parserListener->enterNumberLiteral(this);
}

void CypherParser::NumberLiteralContext::exitRule(
    tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<CypherListener *>(listener);
  if (parserListener != nullptr) parserListener->exitNumberLiteral(this);
}

antlrcpp::Any CypherParser::NumberLiteralContext::accept(
    tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<CypherVisitor *>(visitor))
    return parserVisitor->visitNumberLiteral(this);
  else
    return visitor->visitChildren(this);
}

CypherParser::NumberLiteralContext *CypherParser::numberLiteral() {
  NumberLiteralContext *_localctx =
      _tracker.createInstance<NumberLiteralContext>(_ctx, getState());
  enterRule(_localctx, 136, CypherParser::RuleNumberLiteral);

  auto onExit = finally([=] { exitRule(); });
  try {
    setState(1162);
    _errHandler->sync(this);
    switch (_input->LA(1)) {
      case CypherParser::ExponentDecimalReal:
      case CypherParser::RegularDecimalReal: {
        enterOuterAlt(_localctx, 1);
        setState(1160);
        doubleLiteral();
        break;
      }

      case CypherParser::HexInteger:
      case CypherParser::DecimalInteger:
      case CypherParser::OctalInteger: {
        enterOuterAlt(_localctx, 2);
        setState(1161);
        integerLiteral();
        break;
      }

      default:
        throw NoViableAltException(this);
    }

  } catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- MapLiteralContext
//------------------------------------------------------------------

CypherParser::MapLiteralContext::MapLiteralContext(ParserRuleContext *parent,
                                                   size_t invokingState)
    : ParserRuleContext(parent, invokingState) {}

std::vector<tree::TerminalNode *> CypherParser::MapLiteralContext::SP() {
  return getTokens(CypherParser::SP);
}

tree::TerminalNode *CypherParser::MapLiteralContext::SP(size_t i) {
  return getToken(CypherParser::SP, i);
}

std::vector<CypherParser::PropertyKeyNameContext *>
CypherParser::MapLiteralContext::propertyKeyName() {
  return getRuleContexts<CypherParser::PropertyKeyNameContext>();
}

CypherParser::PropertyKeyNameContext *
CypherParser::MapLiteralContext::propertyKeyName(size_t i) {
  return getRuleContext<CypherParser::PropertyKeyNameContext>(i);
}

std::vector<CypherParser::ExpressionContext *>
CypherParser::MapLiteralContext::expression() {
  return getRuleContexts<CypherParser::ExpressionContext>();
}

CypherParser::ExpressionContext *CypherParser::MapLiteralContext::expression(
    size_t i) {
  return getRuleContext<CypherParser::ExpressionContext>(i);
}

size_t CypherParser::MapLiteralContext::getRuleIndex() const {
  return CypherParser::RuleMapLiteral;
}

void CypherParser::MapLiteralContext::enterRule(
    tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<CypherListener *>(listener);
  if (parserListener != nullptr) parserListener->enterMapLiteral(this);
}

void CypherParser::MapLiteralContext::exitRule(
    tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<CypherListener *>(listener);
  if (parserListener != nullptr) parserListener->exitMapLiteral(this);
}

antlrcpp::Any CypherParser::MapLiteralContext::accept(
    tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<CypherVisitor *>(visitor))
    return parserVisitor->visitMapLiteral(this);
  else
    return visitor->visitChildren(this);
}

CypherParser::MapLiteralContext *CypherParser::mapLiteral() {
  MapLiteralContext *_localctx =
      _tracker.createInstance<MapLiteralContext>(_ctx, getState());
  enterRule(_localctx, 138, CypherParser::RuleMapLiteral);
  size_t _la = 0;

  auto onExit = finally([=] { exitRule(); });
  try {
    enterOuterAlt(_localctx, 1);
    setState(1164);
    match(CypherParser::T__27);
    setState(1166);
    _errHandler->sync(this);

    _la = _input->LA(1);
    if (_la == CypherParser::SP) {
      setState(1165);
      match(CypherParser::SP);
    }
    setState(1201);
    _errHandler->sync(this);

    _la = _input->LA(1);
    if (((((_la - 55) & ~0x3fULL) == 0) &&
         ((1ULL << (_la - 55)) &
          ((1ULL << (CypherParser::HexLetter - 55)) |
           (1ULL << (CypherParser::UNION - 55)) |
           (1ULL << (CypherParser::ALL - 55)) |
           (1ULL << (CypherParser::OPTIONAL - 55)) |
           (1ULL << (CypherParser::MATCH - 55)) |
           (1ULL << (CypherParser::UNWIND - 55)) |
           (1ULL << (CypherParser::AS - 55)) |
           (1ULL << (CypherParser::MERGE - 55)) |
           (1ULL << (CypherParser::ON - 55)) |
           (1ULL << (CypherParser::CREATE - 55)) |
           (1ULL << (CypherParser::SET - 55)) |
           (1ULL << (CypherParser::DETACH - 55)) |
           (1ULL << (CypherParser::DELETE - 55)) |
           (1ULL << (CypherParser::REMOVE - 55)) |
           (1ULL << (CypherParser::WITH - 55)) |
           (1ULL << (CypherParser::DISTINCT - 55)) |
           (1ULL << (CypherParser::RETURN - 55)) |
           (1ULL << (CypherParser::ORDER - 55)) |
           (1ULL << (CypherParser::BY - 55)) |
           (1ULL << (CypherParser::L_SKIP - 55)) |
           (1ULL << (CypherParser::LIMIT - 55)) |
           (1ULL << (CypherParser::ASCENDING - 55)) |
           (1ULL << (CypherParser::ASC - 55)) |
           (1ULL << (CypherParser::DESCENDING - 55)) |
           (1ULL << (CypherParser::DESC - 55)) |
           (1ULL << (CypherParser::WHERE - 55)) |
           (1ULL << (CypherParser::OR - 55)) |
           (1ULL << (CypherParser::XOR - 55)) |
           (1ULL << (CypherParser::AND - 55)) |
           (1ULL << (CypherParser::NOT - 55)) |
           (1ULL << (CypherParser::IN - 55)) |
           (1ULL << (CypherParser::STARTS - 55)) |
           (1ULL << (CypherParser::ENDS - 55)) |
           (1ULL << (CypherParser::CONTAINS - 55)) |
           (1ULL << (CypherParser::IS - 55)) |
           (1ULL << (CypherParser::CYPHERNULL - 55)) |
           (1ULL << (CypherParser::COUNT - 55)) |
           (1ULL << (CypherParser::FILTER - 55)) |
           (1ULL << (CypherParser::EXTRACT - 55)) |
           (1ULL << (CypherParser::ANY - 55)) |
           (1ULL << (CypherParser::NONE - 55)) |
           (1ULL << (CypherParser::SINGLE - 55)) |
           (1ULL << (CypherParser::TRUE - 55)) |
           (1ULL << (CypherParser::FALSE - 55)) |
           (1ULL << (CypherParser::UnescapedSymbolicName - 55)) |
           (1ULL << (CypherParser::EscapedSymbolicName - 55)))) != 0)) {
      setState(1168);
      propertyKeyName();
      setState(1170);
      _errHandler->sync(this);

      _la = _input->LA(1);
      if (_la == CypherParser::SP) {
        setState(1169);
        match(CypherParser::SP);
      }
      setState(1172);
      match(CypherParser::T__10);
      setState(1174);
      _errHandler->sync(this);

      _la = _input->LA(1);
      if (_la == CypherParser::SP) {
        setState(1173);
        match(CypherParser::SP);
      }
      setState(1176);
      expression();
      setState(1178);
      _errHandler->sync(this);

      _la = _input->LA(1);
      if (_la == CypherParser::SP) {
        setState(1177);
        match(CypherParser::SP);
      }
      setState(1198);
      _errHandler->sync(this);
      _la = _input->LA(1);
      while (_la == CypherParser::T__1) {
        setState(1180);
        match(CypherParser::T__1);
        setState(1182);
        _errHandler->sync(this);

        _la = _input->LA(1);
        if (_la == CypherParser::SP) {
          setState(1181);
          match(CypherParser::SP);
        }
        setState(1184);
        propertyKeyName();
        setState(1186);
        _errHandler->sync(this);

        _la = _input->LA(1);
        if (_la == CypherParser::SP) {
          setState(1185);
          match(CypherParser::SP);
        }
        setState(1188);
        match(CypherParser::T__10);
        setState(1190);
        _errHandler->sync(this);

        _la = _input->LA(1);
        if (_la == CypherParser::SP) {
          setState(1189);
          match(CypherParser::SP);
        }
        setState(1192);
        expression();
        setState(1194);
        _errHandler->sync(this);

        _la = _input->LA(1);
        if (_la == CypherParser::SP) {
          setState(1193);
          match(CypherParser::SP);
        }
        setState(1200);
        _errHandler->sync(this);
        _la = _input->LA(1);
      }
    }
    setState(1203);
    match(CypherParser::T__28);

  } catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- ParameterContext
//------------------------------------------------------------------

CypherParser::ParameterContext::ParameterContext(ParserRuleContext *parent,
                                                 size_t invokingState)
    : ParserRuleContext(parent, invokingState) {}

CypherParser::SymbolicNameContext *
CypherParser::ParameterContext::symbolicName() {
  return getRuleContext<CypherParser::SymbolicNameContext>(0);
}

tree::TerminalNode *CypherParser::ParameterContext::DecimalInteger() {
  return getToken(CypherParser::DecimalInteger, 0);
}

size_t CypherParser::ParameterContext::getRuleIndex() const {
  return CypherParser::RuleParameter;
}

void CypherParser::ParameterContext::enterRule(
    tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<CypherListener *>(listener);
  if (parserListener != nullptr) parserListener->enterParameter(this);
}

void CypherParser::ParameterContext::exitRule(
    tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<CypherListener *>(listener);
  if (parserListener != nullptr) parserListener->exitParameter(this);
}

antlrcpp::Any CypherParser::ParameterContext::accept(
    tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<CypherVisitor *>(visitor))
    return parserVisitor->visitParameter(this);
  else
    return visitor->visitChildren(this);
}

CypherParser::ParameterContext *CypherParser::parameter() {
  ParameterContext *_localctx =
      _tracker.createInstance<ParameterContext>(_ctx, getState());
  enterRule(_localctx, 140, CypherParser::RuleParameter);

  auto onExit = finally([=] { exitRule(); });
  try {
    enterOuterAlt(_localctx, 1);
    setState(1205);
    match(CypherParser::T__29);
    setState(1208);
    _errHandler->sync(this);
    switch (_input->LA(1)) {
      case CypherParser::HexLetter:
      case CypherParser::UNION:
      case CypherParser::ALL:
      case CypherParser::OPTIONAL:
      case CypherParser::MATCH:
      case CypherParser::UNWIND:
      case CypherParser::AS:
      case CypherParser::MERGE:
      case CypherParser::ON:
      case CypherParser::CREATE:
      case CypherParser::SET:
      case CypherParser::DETACH:
      case CypherParser::DELETE:
      case CypherParser::REMOVE:
      case CypherParser::WITH:
      case CypherParser::DISTINCT:
      case CypherParser::RETURN:
      case CypherParser::ORDER:
      case CypherParser::BY:
      case CypherParser::L_SKIP:
      case CypherParser::LIMIT:
      case CypherParser::ASCENDING:
      case CypherParser::ASC:
      case CypherParser::DESCENDING:
      case CypherParser::DESC:
      case CypherParser::WHERE:
      case CypherParser::OR:
      case CypherParser::XOR:
      case CypherParser::AND:
      case CypherParser::NOT:
      case CypherParser::IN:
      case CypherParser::STARTS:
      case CypherParser::ENDS:
      case CypherParser::CONTAINS:
      case CypherParser::IS:
      case CypherParser::CYPHERNULL:
      case CypherParser::COUNT:
      case CypherParser::FILTER:
      case CypherParser::EXTRACT:
      case CypherParser::ANY:
      case CypherParser::NONE:
      case CypherParser::SINGLE:
      case CypherParser::TRUE:
      case CypherParser::FALSE:
      case CypherParser::UnescapedSymbolicName:
      case CypherParser::EscapedSymbolicName: {
        setState(1206);
        symbolicName();
        break;
      }

      case CypherParser::DecimalInteger: {
        setState(1207);
        match(CypherParser::DecimalInteger);
        break;
      }

      default:
        throw NoViableAltException(this);
    }

  } catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- PropertyExpressionContext
//------------------------------------------------------------------

CypherParser::PropertyExpressionContext::PropertyExpressionContext(
    ParserRuleContext *parent, size_t invokingState)
    : ParserRuleContext(parent, invokingState) {}

CypherParser::AtomContext *CypherParser::PropertyExpressionContext::atom() {
  return getRuleContext<CypherParser::AtomContext>(0);
}

std::vector<CypherParser::PropertyLookupContext *>
CypherParser::PropertyExpressionContext::propertyLookup() {
  return getRuleContexts<CypherParser::PropertyLookupContext>();
}

CypherParser::PropertyLookupContext *
CypherParser::PropertyExpressionContext::propertyLookup(size_t i) {
  return getRuleContext<CypherParser::PropertyLookupContext>(i);
}

std::vector<tree::TerminalNode *>
CypherParser::PropertyExpressionContext::SP() {
  return getTokens(CypherParser::SP);
}

tree::TerminalNode *CypherParser::PropertyExpressionContext::SP(size_t i) {
  return getToken(CypherParser::SP, i);
}

size_t CypherParser::PropertyExpressionContext::getRuleIndex() const {
  return CypherParser::RulePropertyExpression;
}

void CypherParser::PropertyExpressionContext::enterRule(
    tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<CypherListener *>(listener);
  if (parserListener != nullptr) parserListener->enterPropertyExpression(this);
}

void CypherParser::PropertyExpressionContext::exitRule(
    tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<CypherListener *>(listener);
  if (parserListener != nullptr) parserListener->exitPropertyExpression(this);
}

antlrcpp::Any CypherParser::PropertyExpressionContext::accept(
    tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<CypherVisitor *>(visitor))
    return parserVisitor->visitPropertyExpression(this);
  else
    return visitor->visitChildren(this);
}

CypherParser::PropertyExpressionContext *CypherParser::propertyExpression() {
  PropertyExpressionContext *_localctx =
      _tracker.createInstance<PropertyExpressionContext>(_ctx, getState());
  enterRule(_localctx, 142, CypherParser::RulePropertyExpression);

  auto onExit = finally([=] { exitRule(); });
  try {
    size_t alt;
    enterOuterAlt(_localctx, 1);
    setState(1210);
    atom();
    setState(1215);
    _errHandler->sync(this);
    alt = 1;
    do {
      switch (alt) {
        case 1: {
          setState(1212);
          _errHandler->sync(this);

          switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(
              _input, 217, _ctx)) {
            case 1: {
              setState(1211);
              match(CypherParser::SP);
              break;
            }
          }
          setState(1214);
          propertyLookup();
          break;
        }

        default:
          throw NoViableAltException(this);
      }
      setState(1217);
      _errHandler->sync(this);
      alt = getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(
          _input, 218, _ctx);
    } while (alt != 2 && alt != atn::ATN::INVALID_ALT_NUMBER);

  } catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- PropertyKeyNameContext
//------------------------------------------------------------------

CypherParser::PropertyKeyNameContext::PropertyKeyNameContext(
    ParserRuleContext *parent, size_t invokingState)
    : ParserRuleContext(parent, invokingState) {}

CypherParser::SymbolicNameContext *
CypherParser::PropertyKeyNameContext::symbolicName() {
  return getRuleContext<CypherParser::SymbolicNameContext>(0);
}

size_t CypherParser::PropertyKeyNameContext::getRuleIndex() const {
  return CypherParser::RulePropertyKeyName;
}

void CypherParser::PropertyKeyNameContext::enterRule(
    tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<CypherListener *>(listener);
  if (parserListener != nullptr) parserListener->enterPropertyKeyName(this);
}

void CypherParser::PropertyKeyNameContext::exitRule(
    tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<CypherListener *>(listener);
  if (parserListener != nullptr) parserListener->exitPropertyKeyName(this);
}

antlrcpp::Any CypherParser::PropertyKeyNameContext::accept(
    tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<CypherVisitor *>(visitor))
    return parserVisitor->visitPropertyKeyName(this);
  else
    return visitor->visitChildren(this);
}

CypherParser::PropertyKeyNameContext *CypherParser::propertyKeyName() {
  PropertyKeyNameContext *_localctx =
      _tracker.createInstance<PropertyKeyNameContext>(_ctx, getState());
  enterRule(_localctx, 144, CypherParser::RulePropertyKeyName);

  auto onExit = finally([=] { exitRule(); });
  try {
    enterOuterAlt(_localctx, 1);
    setState(1219);
    symbolicName();

  } catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- IntegerLiteralContext
//------------------------------------------------------------------

CypherParser::IntegerLiteralContext::IntegerLiteralContext(
    ParserRuleContext *parent, size_t invokingState)
    : ParserRuleContext(parent, invokingState) {}

tree::TerminalNode *CypherParser::IntegerLiteralContext::HexInteger() {
  return getToken(CypherParser::HexInteger, 0);
}

tree::TerminalNode *CypherParser::IntegerLiteralContext::OctalInteger() {
  return getToken(CypherParser::OctalInteger, 0);
}

tree::TerminalNode *CypherParser::IntegerLiteralContext::DecimalInteger() {
  return getToken(CypherParser::DecimalInteger, 0);
}

size_t CypherParser::IntegerLiteralContext::getRuleIndex() const {
  return CypherParser::RuleIntegerLiteral;
}

void CypherParser::IntegerLiteralContext::enterRule(
    tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<CypherListener *>(listener);
  if (parserListener != nullptr) parserListener->enterIntegerLiteral(this);
}

void CypherParser::IntegerLiteralContext::exitRule(
    tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<CypherListener *>(listener);
  if (parserListener != nullptr) parserListener->exitIntegerLiteral(this);
}

antlrcpp::Any CypherParser::IntegerLiteralContext::accept(
    tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<CypherVisitor *>(visitor))
    return parserVisitor->visitIntegerLiteral(this);
  else
    return visitor->visitChildren(this);
}

CypherParser::IntegerLiteralContext *CypherParser::integerLiteral() {
  IntegerLiteralContext *_localctx =
      _tracker.createInstance<IntegerLiteralContext>(_ctx, getState());
  enterRule(_localctx, 146, CypherParser::RuleIntegerLiteral);
  size_t _la = 0;

  auto onExit = finally([=] { exitRule(); });
  try {
    enterOuterAlt(_localctx, 1);
    setState(1221);
    _la = _input->LA(1);
    if (!((((_la & ~0x3fULL) == 0) &&
           ((1ULL << _la) & ((1ULL << CypherParser::HexInteger) |
                             (1ULL << CypherParser::DecimalInteger) |
                             (1ULL << CypherParser::OctalInteger))) != 0))) {
      _errHandler->recoverInline(this);
    } else {
      _errHandler->reportMatch(this);
      consume();
    }

  } catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- DoubleLiteralContext
//------------------------------------------------------------------

CypherParser::DoubleLiteralContext::DoubleLiteralContext(
    ParserRuleContext *parent, size_t invokingState)
    : ParserRuleContext(parent, invokingState) {}

tree::TerminalNode *CypherParser::DoubleLiteralContext::ExponentDecimalReal() {
  return getToken(CypherParser::ExponentDecimalReal, 0);
}

tree::TerminalNode *CypherParser::DoubleLiteralContext::RegularDecimalReal() {
  return getToken(CypherParser::RegularDecimalReal, 0);
}

size_t CypherParser::DoubleLiteralContext::getRuleIndex() const {
  return CypherParser::RuleDoubleLiteral;
}

void CypherParser::DoubleLiteralContext::enterRule(
    tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<CypherListener *>(listener);
  if (parserListener != nullptr) parserListener->enterDoubleLiteral(this);
}

void CypherParser::DoubleLiteralContext::exitRule(
    tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<CypherListener *>(listener);
  if (parserListener != nullptr) parserListener->exitDoubleLiteral(this);
}

antlrcpp::Any CypherParser::DoubleLiteralContext::accept(
    tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<CypherVisitor *>(visitor))
    return parserVisitor->visitDoubleLiteral(this);
  else
    return visitor->visitChildren(this);
}

CypherParser::DoubleLiteralContext *CypherParser::doubleLiteral() {
  DoubleLiteralContext *_localctx =
      _tracker.createInstance<DoubleLiteralContext>(_ctx, getState());
  enterRule(_localctx, 148, CypherParser::RuleDoubleLiteral);
  size_t _la = 0;

  auto onExit = finally([=] { exitRule(); });
  try {
    enterOuterAlt(_localctx, 1);
    setState(1223);
    _la = _input->LA(1);
    if (!(_la == CypherParser::ExponentDecimalReal

          || _la == CypherParser::RegularDecimalReal)) {
      _errHandler->recoverInline(this);
    } else {
      _errHandler->reportMatch(this);
      consume();
    }

  } catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- SymbolicNameContext
//------------------------------------------------------------------

CypherParser::SymbolicNameContext::SymbolicNameContext(
    ParserRuleContext *parent, size_t invokingState)
    : ParserRuleContext(parent, invokingState) {}

tree::TerminalNode *CypherParser::SymbolicNameContext::UnescapedSymbolicName() {
  return getToken(CypherParser::UnescapedSymbolicName, 0);
}

tree::TerminalNode *CypherParser::SymbolicNameContext::EscapedSymbolicName() {
  return getToken(CypherParser::EscapedSymbolicName, 0);
}

tree::TerminalNode *CypherParser::SymbolicNameContext::UNION() {
  return getToken(CypherParser::UNION, 0);
}

tree::TerminalNode *CypherParser::SymbolicNameContext::ALL() {
  return getToken(CypherParser::ALL, 0);
}

tree::TerminalNode *CypherParser::SymbolicNameContext::OPTIONAL() {
  return getToken(CypherParser::OPTIONAL, 0);
}

tree::TerminalNode *CypherParser::SymbolicNameContext::MATCH() {
  return getToken(CypherParser::MATCH, 0);
}

tree::TerminalNode *CypherParser::SymbolicNameContext::UNWIND() {
  return getToken(CypherParser::UNWIND, 0);
}

tree::TerminalNode *CypherParser::SymbolicNameContext::AS() {
  return getToken(CypherParser::AS, 0);
}

tree::TerminalNode *CypherParser::SymbolicNameContext::MERGE() {
  return getToken(CypherParser::MERGE, 0);
}

tree::TerminalNode *CypherParser::SymbolicNameContext::ON() {
  return getToken(CypherParser::ON, 0);
}

tree::TerminalNode *CypherParser::SymbolicNameContext::CREATE() {
  return getToken(CypherParser::CREATE, 0);
}

tree::TerminalNode *CypherParser::SymbolicNameContext::SET() {
  return getToken(CypherParser::SET, 0);
}

tree::TerminalNode *CypherParser::SymbolicNameContext::DETACH() {
  return getToken(CypherParser::DETACH, 0);
}

tree::TerminalNode *CypherParser::SymbolicNameContext::DELETE() {
  return getToken(CypherParser::DELETE, 0);
}

tree::TerminalNode *CypherParser::SymbolicNameContext::REMOVE() {
  return getToken(CypherParser::REMOVE, 0);
}

tree::TerminalNode *CypherParser::SymbolicNameContext::WITH() {
  return getToken(CypherParser::WITH, 0);
}

tree::TerminalNode *CypherParser::SymbolicNameContext::DISTINCT() {
  return getToken(CypherParser::DISTINCT, 0);
}

tree::TerminalNode *CypherParser::SymbolicNameContext::RETURN() {
  return getToken(CypherParser::RETURN, 0);
}

tree::TerminalNode *CypherParser::SymbolicNameContext::ORDER() {
  return getToken(CypherParser::ORDER, 0);
}

tree::TerminalNode *CypherParser::SymbolicNameContext::BY() {
  return getToken(CypherParser::BY, 0);
}

tree::TerminalNode *CypherParser::SymbolicNameContext::L_SKIP() {
  return getToken(CypherParser::L_SKIP, 0);
}

tree::TerminalNode *CypherParser::SymbolicNameContext::LIMIT() {
  return getToken(CypherParser::LIMIT, 0);
}

tree::TerminalNode *CypherParser::SymbolicNameContext::ASCENDING() {
  return getToken(CypherParser::ASCENDING, 0);
}

tree::TerminalNode *CypherParser::SymbolicNameContext::ASC() {
  return getToken(CypherParser::ASC, 0);
}

tree::TerminalNode *CypherParser::SymbolicNameContext::DESCENDING() {
  return getToken(CypherParser::DESCENDING, 0);
}

tree::TerminalNode *CypherParser::SymbolicNameContext::DESC() {
  return getToken(CypherParser::DESC, 0);
}

tree::TerminalNode *CypherParser::SymbolicNameContext::WHERE() {
  return getToken(CypherParser::WHERE, 0);
}

tree::TerminalNode *CypherParser::SymbolicNameContext::OR() {
  return getToken(CypherParser::OR, 0);
}

tree::TerminalNode *CypherParser::SymbolicNameContext::XOR() {
  return getToken(CypherParser::XOR, 0);
}

tree::TerminalNode *CypherParser::SymbolicNameContext::AND() {
  return getToken(CypherParser::AND, 0);
}

tree::TerminalNode *CypherParser::SymbolicNameContext::NOT() {
  return getToken(CypherParser::NOT, 0);
}

tree::TerminalNode *CypherParser::SymbolicNameContext::IN() {
  return getToken(CypherParser::IN, 0);
}

tree::TerminalNode *CypherParser::SymbolicNameContext::STARTS() {
  return getToken(CypherParser::STARTS, 0);
}

tree::TerminalNode *CypherParser::SymbolicNameContext::ENDS() {
  return getToken(CypherParser::ENDS, 0);
}

tree::TerminalNode *CypherParser::SymbolicNameContext::CONTAINS() {
  return getToken(CypherParser::CONTAINS, 0);
}

tree::TerminalNode *CypherParser::SymbolicNameContext::IS() {
  return getToken(CypherParser::IS, 0);
}

tree::TerminalNode *CypherParser::SymbolicNameContext::CYPHERNULL() {
  return getToken(CypherParser::CYPHERNULL, 0);
}

tree::TerminalNode *CypherParser::SymbolicNameContext::COUNT() {
  return getToken(CypherParser::COUNT, 0);
}

tree::TerminalNode *CypherParser::SymbolicNameContext::FILTER() {
  return getToken(CypherParser::FILTER, 0);
}

tree::TerminalNode *CypherParser::SymbolicNameContext::EXTRACT() {
  return getToken(CypherParser::EXTRACT, 0);
}

tree::TerminalNode *CypherParser::SymbolicNameContext::ANY() {
  return getToken(CypherParser::ANY, 0);
}

tree::TerminalNode *CypherParser::SymbolicNameContext::NONE() {
  return getToken(CypherParser::NONE, 0);
}

tree::TerminalNode *CypherParser::SymbolicNameContext::SINGLE() {
  return getToken(CypherParser::SINGLE, 0);
}

tree::TerminalNode *CypherParser::SymbolicNameContext::TRUE() {
  return getToken(CypherParser::TRUE, 0);
}

tree::TerminalNode *CypherParser::SymbolicNameContext::FALSE() {
  return getToken(CypherParser::FALSE, 0);
}

tree::TerminalNode *CypherParser::SymbolicNameContext::HexLetter() {
  return getToken(CypherParser::HexLetter, 0);
}

size_t CypherParser::SymbolicNameContext::getRuleIndex() const {
  return CypherParser::RuleSymbolicName;
}

void CypherParser::SymbolicNameContext::enterRule(
    tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<CypherListener *>(listener);
  if (parserListener != nullptr) parserListener->enterSymbolicName(this);
}

void CypherParser::SymbolicNameContext::exitRule(
    tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<CypherListener *>(listener);
  if (parserListener != nullptr) parserListener->exitSymbolicName(this);
}

antlrcpp::Any CypherParser::SymbolicNameContext::accept(
    tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<CypherVisitor *>(visitor))
    return parserVisitor->visitSymbolicName(this);
  else
    return visitor->visitChildren(this);
}

CypherParser::SymbolicNameContext *CypherParser::symbolicName() {
  SymbolicNameContext *_localctx =
      _tracker.createInstance<SymbolicNameContext>(_ctx, getState());
  enterRule(_localctx, 150, CypherParser::RuleSymbolicName);
  size_t _la = 0;

  auto onExit = finally([=] { exitRule(); });
  try {
    enterOuterAlt(_localctx, 1);
    setState(1225);
    _la = _input->LA(1);
    if (!(((((_la - 55) & ~0x3fULL) == 0) &&
           ((1ULL << (_la - 55)) &
            ((1ULL << (CypherParser::HexLetter - 55)) |
             (1ULL << (CypherParser::UNION - 55)) |
             (1ULL << (CypherParser::ALL - 55)) |
             (1ULL << (CypherParser::OPTIONAL - 55)) |
             (1ULL << (CypherParser::MATCH - 55)) |
             (1ULL << (CypherParser::UNWIND - 55)) |
             (1ULL << (CypherParser::AS - 55)) |
             (1ULL << (CypherParser::MERGE - 55)) |
             (1ULL << (CypherParser::ON - 55)) |
             (1ULL << (CypherParser::CREATE - 55)) |
             (1ULL << (CypherParser::SET - 55)) |
             (1ULL << (CypherParser::DETACH - 55)) |
             (1ULL << (CypherParser::DELETE - 55)) |
             (1ULL << (CypherParser::REMOVE - 55)) |
             (1ULL << (CypherParser::WITH - 55)) |
             (1ULL << (CypherParser::DISTINCT - 55)) |
             (1ULL << (CypherParser::RETURN - 55)) |
             (1ULL << (CypherParser::ORDER - 55)) |
             (1ULL << (CypherParser::BY - 55)) |
             (1ULL << (CypherParser::L_SKIP - 55)) |
             (1ULL << (CypherParser::LIMIT - 55)) |
             (1ULL << (CypherParser::ASCENDING - 55)) |
             (1ULL << (CypherParser::ASC - 55)) |
             (1ULL << (CypherParser::DESCENDING - 55)) |
             (1ULL << (CypherParser::DESC - 55)) |
             (1ULL << (CypherParser::WHERE - 55)) |
             (1ULL << (CypherParser::OR - 55)) |
             (1ULL << (CypherParser::XOR - 55)) |
             (1ULL << (CypherParser::AND - 55)) |
             (1ULL << (CypherParser::NOT - 55)) |
             (1ULL << (CypherParser::IN - 55)) |
             (1ULL << (CypherParser::STARTS - 55)) |
             (1ULL << (CypherParser::ENDS - 55)) |
             (1ULL << (CypherParser::CONTAINS - 55)) |
             (1ULL << (CypherParser::IS - 55)) |
             (1ULL << (CypherParser::CYPHERNULL - 55)) |
             (1ULL << (CypherParser::COUNT - 55)) |
             (1ULL << (CypherParser::FILTER - 55)) |
             (1ULL << (CypherParser::EXTRACT - 55)) |
             (1ULL << (CypherParser::ANY - 55)) |
             (1ULL << (CypherParser::NONE - 55)) |
             (1ULL << (CypherParser::SINGLE - 55)) |
             (1ULL << (CypherParser::TRUE - 55)) |
             (1ULL << (CypherParser::FALSE - 55)) |
             (1ULL << (CypherParser::UnescapedSymbolicName - 55)) |
             (1ULL << (CypherParser::EscapedSymbolicName - 55)))) != 0))) {
      _errHandler->recoverInline(this);
    } else {
      _errHandler->reportMatch(this);
      consume();
    }

  } catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- LeftArrowHeadContext
//------------------------------------------------------------------

CypherParser::LeftArrowHeadContext::LeftArrowHeadContext(
    ParserRuleContext *parent, size_t invokingState)
    : ParserRuleContext(parent, invokingState) {}

size_t CypherParser::LeftArrowHeadContext::getRuleIndex() const {
  return CypherParser::RuleLeftArrowHead;
}

void CypherParser::LeftArrowHeadContext::enterRule(
    tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<CypherListener *>(listener);
  if (parserListener != nullptr) parserListener->enterLeftArrowHead(this);
}

void CypherParser::LeftArrowHeadContext::exitRule(
    tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<CypherListener *>(listener);
  if (parserListener != nullptr) parserListener->exitLeftArrowHead(this);
}

antlrcpp::Any CypherParser::LeftArrowHeadContext::accept(
    tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<CypherVisitor *>(visitor))
    return parserVisitor->visitLeftArrowHead(this);
  else
    return visitor->visitChildren(this);
}

CypherParser::LeftArrowHeadContext *CypherParser::leftArrowHead() {
  LeftArrowHeadContext *_localctx =
      _tracker.createInstance<LeftArrowHeadContext>(_ctx, getState());
  enterRule(_localctx, 152, CypherParser::RuleLeftArrowHead);
  size_t _la = 0;

  auto onExit = finally([=] { exitRule(); });
  try {
    enterOuterAlt(_localctx, 1);
    setState(1227);
    _la = _input->LA(1);
    if (!((((_la & ~0x3fULL) == 0) &&
           ((1ULL << _la) &
            ((1ULL << CypherParser::T__21) | (1ULL << CypherParser::T__30) |
             (1ULL << CypherParser::T__31) | (1ULL << CypherParser::T__32) |
             (1ULL << CypherParser::T__33))) != 0))) {
      _errHandler->recoverInline(this);
    } else {
      _errHandler->reportMatch(this);
      consume();
    }

  } catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- RightArrowHeadContext
//------------------------------------------------------------------

CypherParser::RightArrowHeadContext::RightArrowHeadContext(
    ParserRuleContext *parent, size_t invokingState)
    : ParserRuleContext(parent, invokingState) {}

size_t CypherParser::RightArrowHeadContext::getRuleIndex() const {
  return CypherParser::RuleRightArrowHead;
}

void CypherParser::RightArrowHeadContext::enterRule(
    tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<CypherListener *>(listener);
  if (parserListener != nullptr) parserListener->enterRightArrowHead(this);
}

void CypherParser::RightArrowHeadContext::exitRule(
    tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<CypherListener *>(listener);
  if (parserListener != nullptr) parserListener->exitRightArrowHead(this);
}

antlrcpp::Any CypherParser::RightArrowHeadContext::accept(
    tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<CypherVisitor *>(visitor))
    return parserVisitor->visitRightArrowHead(this);
  else
    return visitor->visitChildren(this);
}

CypherParser::RightArrowHeadContext *CypherParser::rightArrowHead() {
  RightArrowHeadContext *_localctx =
      _tracker.createInstance<RightArrowHeadContext>(_ctx, getState());
  enterRule(_localctx, 154, CypherParser::RuleRightArrowHead);
  size_t _la = 0;

  auto onExit = finally([=] { exitRule(); });
  try {
    enterOuterAlt(_localctx, 1);
    setState(1229);
    _la = _input->LA(1);
    if (!((((_la & ~0x3fULL) == 0) &&
           ((1ULL << _la) &
            ((1ULL << CypherParser::T__22) | (1ULL << CypherParser::T__34) |
             (1ULL << CypherParser::T__35) | (1ULL << CypherParser::T__36) |
             (1ULL << CypherParser::T__37))) != 0))) {
      _errHandler->recoverInline(this);
    } else {
      _errHandler->reportMatch(this);
      consume();
    }

  } catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- DashContext
//------------------------------------------------------------------

CypherParser::DashContext::DashContext(ParserRuleContext *parent,
                                       size_t invokingState)
    : ParserRuleContext(parent, invokingState) {}

size_t CypherParser::DashContext::getRuleIndex() const {
  return CypherParser::RuleDash;
}

void CypherParser::DashContext::enterRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<CypherListener *>(listener);
  if (parserListener != nullptr) parserListener->enterDash(this);
}

void CypherParser::DashContext::exitRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<CypherListener *>(listener);
  if (parserListener != nullptr) parserListener->exitDash(this);
}

antlrcpp::Any CypherParser::DashContext::accept(
    tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<CypherVisitor *>(visitor))
    return parserVisitor->visitDash(this);
  else
    return visitor->visitChildren(this);
}

CypherParser::DashContext *CypherParser::dash() {
  DashContext *_localctx =
      _tracker.createInstance<DashContext>(_ctx, getState());
  enterRule(_localctx, 156, CypherParser::RuleDash);
  size_t _la = 0;

  auto onExit = finally([=] { exitRule(); });
  try {
    enterOuterAlt(_localctx, 1);
    setState(1231);
    _la = _input->LA(1);
    if (!((((_la & ~0x3fULL) == 0) &&
           ((1ULL << _la) &
            ((1ULL << CypherParser::T__14) | (1ULL << CypherParser::T__38) |
             (1ULL << CypherParser::T__39) | (1ULL << CypherParser::T__40) |
             (1ULL << CypherParser::T__41) | (1ULL << CypherParser::T__42) |
             (1ULL << CypherParser::T__43) | (1ULL << CypherParser::T__44) |
             (1ULL << CypherParser::T__45) | (1ULL << CypherParser::T__46) |
             (1ULL << CypherParser::T__47) | (1ULL << CypherParser::T__48))) !=
               0))) {
      _errHandler->recoverInline(this);
    } else {
      _errHandler->reportMatch(this);
      consume();
    }

  } catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

// Static vars and initialization.
std::vector<dfa::DFA> CypherParser::_decisionToDFA;
atn::PredictionContextCache CypherParser::_sharedContextCache;

// We own the ATN which in turn owns the ATN states.
atn::ATN CypherParser::_atn;
std::vector<uint16_t> CypherParser::_serializedATN;

std::vector<std::string> CypherParser::_ruleNames = {
    "cypher",
    "statement",
    "query",
    "regularQuery",
    "singleQuery",
    "cypherUnion",
    "clause",
    "cypherMatch",
    "unwind",
    "merge",
    "mergeAction",
    "create",
    "set",
    "setItem",
    "cypherDelete",
    "remove",
    "removeItem",
    "with",
    "cypherReturn",
    "returnBody",
    "returnItems",
    "returnItem",
    "order",
    "skip",
    "limit",
    "sortItem",
    "where",
    "pattern",
    "patternPart",
    "anonymousPatternPart",
    "patternElement",
    "nodePattern",
    "patternElementChain",
    "relationshipPattern",
    "relationshipDetail",
    "properties",
    "relationshipTypes",
    "nodeLabels",
    "nodeLabel",
    "rangeLiteral",
    "labelName",
    "relTypeName",
    "expression",
    "expression12",
    "expression11",
    "expression10",
    "expression9",
    "expression8",
    "expression7",
    "expression6",
    "expression5",
    "expression4",
    "expression3",
    "expression2",
    "atom",
    "literal",
    "booleanLiteral",
    "listLiteral",
    "partialComparisonExpression",
    "parenthesizedExpression",
    "relationshipsPattern",
    "filterExpression",
    "idInColl",
    "functionInvocation",
    "functionName",
    "listComprehension",
    "propertyLookup",
    "variable",
    "numberLiteral",
    "mapLiteral",
    "parameter",
    "propertyExpression",
    "propertyKeyName",
    "integerLiteral",
    "doubleLiteral",
    "symbolicName",
    "leftArrowHead",
    "rightArrowHead",
    "dash"};

std::vector<std::string> CypherParser::_literalNames = {
    "",     "';'",  "','",  "'='",  "'+='", "'*'",  "'('",  "')'",  "'['",
    "'?'",  "']'",  "':'",  "'|'",  "'..'", "'+'",  "'-'",  "'/'",  "'%'",
    "'^'",  "'=~'", "'<>'", "'!='", "'<'",  "'>'",  "'<='", "'>='", "'.'",
    "'!'",  "'{'",  "'}'",  "'$'",  "'⟨'",  "'〈'", "'﹤'", "'＜'", "'⟩'",
    "'〉'", "'﹥'", "'＞'", "'­'",  "'‐'",  "'‑'",  "'‒'",  "'–'",  "'—'",
    "'―'",  "'−'",  "'﹘'", "'﹣'", "'－'", "",     "",     "",     "",
    "",     "",     "",     "",     "",     "",     "",     "'0'"};

std::vector<std::string> CypherParser::_symbolicNames = {
    "",
    "",
    "",
    "",
    "",
    "",
    "",
    "",
    "",
    "",
    "",
    "",
    "",
    "",
    "",
    "",
    "",
    "",
    "",
    "",
    "",
    "",
    "",
    "",
    "",
    "",
    "",
    "",
    "",
    "",
    "",
    "",
    "",
    "",
    "",
    "",
    "",
    "",
    "",
    "",
    "",
    "",
    "",
    "",
    "",
    "",
    "",
    "",
    "",
    "",
    "StringLiteral",
    "EscapedChar",
    "HexInteger",
    "DecimalInteger",
    "OctalInteger",
    "HexLetter",
    "HexDigit",
    "Digit",
    "NonZeroDigit",
    "NonZeroOctDigit",
    "OctDigit",
    "ZeroDigit",
    "ExponentDecimalReal",
    "RegularDecimalReal",
    "UNION",
    "ALL",
    "OPTIONAL",
    "MATCH",
    "UNWIND",
    "AS",
    "MERGE",
    "ON",
    "CREATE",
    "SET",
    "DETACH",
    "DELETE",
    "REMOVE",
    "WITH",
    "DISTINCT",
    "RETURN",
    "ORDER",
    "BY",
    "L_SKIP",
    "LIMIT",
    "ASCENDING",
    "ASC",
    "DESCENDING",
    "DESC",
    "WHERE",
    "OR",
    "XOR",
    "AND",
    "NOT",
    "IN",
    "STARTS",
    "ENDS",
    "CONTAINS",
    "IS",
    "CYPHERNULL",
    "COUNT",
    "FILTER",
    "EXTRACT",
    "ANY",
    "NONE",
    "SINGLE",
    "TRUE",
    "FALSE",
    "UnescapedSymbolicName",
    "IdentifierStart",
    "IdentifierPart",
    "EscapedSymbolicName",
    "SP",
    "WHITESPACE",
    "Comment",
    "L_0X"};

dfa::Vocabulary CypherParser::_vocabulary(_literalNames, _symbolicNames);

std::vector<std::string> CypherParser::_tokenNames;

CypherParser::Initializer::Initializer() {
  for (size_t i = 0; i < _symbolicNames.size(); ++i) {
    std::string name = _vocabulary.getLiteralName(i);
    if (name.empty()) {
      name = _vocabulary.getSymbolicName(i);
    }

    if (name.empty()) {
      _tokenNames.push_back("<INVALID>");
    } else {
      _tokenNames.push_back(name);
    }
  }

  _serializedATN = {
      0x3,   0x430, 0xd6d1, 0x8206, 0xad2d, 0x4417, 0xaef1, 0x8d80, 0xaadd,
      0x3,   0x74,  0x4d4,  0x4,    0x2,    0x9,    0x2,    0x4,    0x3,
      0x9,   0x3,   0x4,    0x4,    0x9,    0x4,    0x4,    0x5,    0x9,
      0x5,   0x4,   0x6,    0x9,    0x6,    0x4,    0x7,    0x9,    0x7,
      0x4,   0x8,   0x9,    0x8,    0x4,    0x9,    0x9,    0x9,    0x4,
      0xa,   0x9,   0xa,    0x4,    0xb,    0x9,    0xb,    0x4,    0xc,
      0x9,   0xc,   0x4,    0xd,    0x9,    0xd,    0x4,    0xe,    0x9,
      0xe,   0x4,   0xf,    0x9,    0xf,    0x4,    0x10,   0x9,    0x10,
      0x4,   0x11,  0x9,    0x11,   0x4,    0x12,   0x9,    0x12,   0x4,
      0x13,  0x9,   0x13,   0x4,    0x14,   0x9,    0x14,   0x4,    0x15,
      0x9,   0x15,  0x4,    0x16,   0x9,    0x16,   0x4,    0x17,   0x9,
      0x17,  0x4,   0x18,   0x9,    0x18,   0x4,    0x19,   0x9,    0x19,
      0x4,   0x1a,  0x9,    0x1a,   0x4,    0x1b,   0x9,    0x1b,   0x4,
      0x1c,  0x9,   0x1c,   0x4,    0x1d,   0x9,    0x1d,   0x4,    0x1e,
      0x9,   0x1e,  0x4,    0x1f,   0x9,    0x1f,   0x4,    0x20,   0x9,
      0x20,  0x4,   0x21,   0x9,    0x21,   0x4,    0x22,   0x9,    0x22,
      0x4,   0x23,  0x9,    0x23,   0x4,    0x24,   0x9,    0x24,   0x4,
      0x25,  0x9,   0x25,   0x4,    0x26,   0x9,    0x26,   0x4,    0x27,
      0x9,   0x27,  0x4,    0x28,   0x9,    0x28,   0x4,    0x29,   0x9,
      0x29,  0x4,   0x2a,   0x9,    0x2a,   0x4,    0x2b,   0x9,    0x2b,
      0x4,   0x2c,  0x9,    0x2c,   0x4,    0x2d,   0x9,    0x2d,   0x4,
      0x2e,  0x9,   0x2e,   0x4,    0x2f,   0x9,    0x2f,   0x4,    0x30,
      0x9,   0x30,  0x4,    0x31,   0x9,    0x31,   0x4,    0x32,   0x9,
      0x32,  0x4,   0x33,   0x9,    0x33,   0x4,    0x34,   0x9,    0x34,
      0x4,   0x35,  0x9,    0x35,   0x4,    0x36,   0x9,    0x36,   0x4,
      0x37,  0x9,   0x37,   0x4,    0x38,   0x9,    0x38,   0x4,    0x39,
      0x9,   0x39,  0x4,    0x3a,   0x9,    0x3a,   0x4,    0x3b,   0x9,
      0x3b,  0x4,   0x3c,   0x9,    0x3c,   0x4,    0x3d,   0x9,    0x3d,
      0x4,   0x3e,  0x9,    0x3e,   0x4,    0x3f,   0x9,    0x3f,   0x4,
      0x40,  0x9,   0x40,   0x4,    0x41,   0x9,    0x41,   0x4,    0x42,
      0x9,   0x42,  0x4,    0x43,   0x9,    0x43,   0x4,    0x44,   0x9,
      0x44,  0x4,   0x45,   0x9,    0x45,   0x4,    0x46,   0x9,    0x46,
      0x4,   0x47,  0x9,    0x47,   0x4,    0x48,   0x9,    0x48,   0x4,
      0x49,  0x9,   0x49,   0x4,    0x4a,   0x9,    0x4a,   0x4,    0x4b,
      0x9,   0x4b,  0x4,    0x4c,   0x9,    0x4c,   0x4,    0x4d,   0x9,
      0x4d,  0x4,   0x4e,   0x9,    0x4e,   0x4,    0x4f,   0x9,    0x4f,
      0x4,   0x50,  0x9,    0x50,   0x3,    0x2,    0x5,    0x2,    0xa2,
      0xa,   0x2,   0x3,    0x2,    0x3,    0x2,    0x5,    0x2,    0xa6,
      0xa,   0x2,   0x3,    0x2,    0x5,    0x2,    0xa9,   0xa,    0x2,
      0x3,   0x2,   0x5,    0x2,    0xac,   0xa,    0x2,    0x3,    0x3,
      0x3,   0x3,   0x3,    0x4,    0x3,    0x4,    0x3,    0x5,    0x3,
      0x5,   0x5,   0x5,    0xb4,   0xa,    0x5,    0x3,    0x5,    0x7,
      0x5,   0xb7,  0xa,    0x5,    0xc,    0x5,    0xe,    0x5,    0xba,
      0xb,   0x5,   0x3,    0x6,    0x3,    0x6,    0x5,    0x6,    0xbe,
      0xa,   0x6,   0x3,    0x6,    0x7,    0x6,    0xc1,   0xa,    0x6,
      0xc,   0x6,   0xe,    0x6,    0xc4,   0xb,    0x6,    0x3,    0x7,
      0x3,   0x7,   0x3,    0x7,    0x3,    0x7,    0x5,    0x7,    0xca,
      0xa,   0x7,   0x3,    0x7,    0x3,    0x7,    0x3,    0x7,    0x5,
      0x7,   0xcf,  0xa,    0x7,    0x3,    0x7,    0x5,    0x7,    0xd2,
      0xa,   0x7,   0x3,    0x8,    0x3,    0x8,    0x3,    0x8,    0x3,
      0x8,   0x3,   0x8,    0x3,    0x8,    0x3,    0x8,    0x3,    0x8,
      0x3,   0x8,   0x5,    0x8,    0xdd,   0xa,    0x8,    0x3,    0x9,
      0x3,   0x9,   0x5,    0x9,    0xe1,   0xa,    0x9,    0x3,    0x9,
      0x3,   0x9,   0x5,    0x9,    0xe5,   0xa,    0x9,    0x3,    0x9,
      0x3,   0x9,   0x5,    0x9,    0xe9,   0xa,    0x9,    0x3,    0x9,
      0x5,   0x9,   0xec,   0xa,    0x9,    0x3,    0xa,    0x3,    0xa,
      0x5,   0xa,   0xf0,   0xa,    0xa,    0x3,    0xa,    0x3,    0xa,
      0x3,   0xa,   0x3,    0xa,    0x3,    0xa,    0x3,    0xa,    0x3,
      0xb,   0x3,   0xb,    0x5,    0xb,    0xfa,   0xa,    0xb,    0x3,
      0xb,   0x3,   0xb,    0x3,    0xb,    0x7,    0xb,    0xff,   0xa,
      0xb,   0xc,   0xb,    0xe,    0xb,    0x102,  0xb,    0xb,    0x3,
      0xc,   0x3,   0xc,    0x3,    0xc,    0x3,    0xc,    0x3,    0xc,
      0x3,   0xc,   0x3,    0xc,    0x3,    0xc,    0x3,    0xc,    0x3,
      0xc,   0x5,   0xc,    0x10e,  0xa,    0xc,    0x3,    0xd,    0x3,
      0xd,   0x5,   0xd,    0x112,  0xa,    0xd,    0x3,    0xd,    0x3,
      0xd,   0x3,   0xe,    0x3,    0xe,    0x3,    0xe,    0x3,    0xe,
      0x7,   0xe,   0x11a,  0xa,    0xe,    0xc,    0xe,    0xe,    0xe,
      0x11d, 0xb,   0xe,    0x3,    0xf,    0x3,    0xf,    0x3,    0xf,
      0x3,   0xf,   0x3,    0xf,    0x3,    0xf,    0x3,    0xf,    0x3,
      0xf,   0x3,   0xf,    0x3,    0xf,    0x3,    0xf,    0x3,    0xf,
      0x3,   0xf,   0x3,    0xf,    0x3,    0xf,    0x5,    0xf,    0x12e,
      0xa,   0xf,   0x3,    0x10,   0x3,    0x10,   0x5,    0x10,   0x132,
      0xa,   0x10,  0x3,    0x10,   0x3,    0x10,   0x5,    0x10,   0x136,
      0xa,   0x10,  0x3,    0x10,   0x3,    0x10,   0x5,    0x10,   0x13a,
      0xa,   0x10,  0x3,    0x10,   0x3,    0x10,   0x5,    0x10,   0x13e,
      0xa,   0x10,  0x3,    0x10,   0x7,    0x10,   0x141,  0xa,    0x10,
      0xc,   0x10,  0xe,    0x10,   0x144,  0xb,    0x10,   0x3,    0x11,
      0x3,   0x11,  0x3,    0x11,   0x3,    0x11,   0x5,    0x11,   0x14a,
      0xa,   0x11,  0x3,    0x11,   0x3,    0x11,   0x5,    0x11,   0x14e,
      0xa,   0x11,  0x3,    0x11,   0x7,    0x11,   0x151,  0xa,    0x11,
      0xc,   0x11,  0xe,    0x11,   0x154,  0xb,    0x11,   0x3,    0x12,
      0x3,   0x12,  0x3,    0x12,   0x3,    0x12,   0x5,    0x12,   0x15a,
      0xa,   0x12,  0x3,    0x13,   0x3,    0x13,   0x5,    0x13,   0x15e,
      0xa,   0x13,  0x3,    0x13,   0x5,    0x13,   0x161,  0xa,    0x13,
      0x3,   0x13,  0x3,    0x13,   0x3,    0x13,   0x5,    0x13,   0x166,
      0xa,   0x13,  0x3,    0x13,   0x5,    0x13,   0x169,  0xa,    0x13,
      0x3,   0x14,  0x3,    0x14,   0x5,    0x14,   0x16d,  0xa,    0x14,
      0x3,   0x14,  0x5,    0x14,   0x170,  0xa,    0x14,   0x3,    0x14,
      0x3,   0x14,  0x3,    0x14,   0x3,    0x15,   0x3,    0x15,   0x3,
      0x15,  0x5,   0x15,   0x178,  0xa,    0x15,   0x3,    0x15,   0x3,
      0x15,  0x5,   0x15,   0x17c,  0xa,    0x15,   0x3,    0x15,   0x3,
      0x15,  0x5,   0x15,   0x180,  0xa,    0x15,   0x3,    0x16,   0x3,
      0x16,  0x5,   0x16,   0x184,  0xa,    0x16,   0x3,    0x16,   0x3,
      0x16,  0x5,   0x16,   0x188,  0xa,    0x16,   0x3,    0x16,   0x7,
      0x16,  0x18b, 0xa,    0x16,   0xc,    0x16,   0xe,    0x16,   0x18e,
      0xb,   0x16,  0x3,    0x16,   0x3,    0x16,   0x5,    0x16,   0x192,
      0xa,   0x16,  0x3,    0x16,   0x3,    0x16,   0x5,    0x16,   0x196,
      0xa,   0x16,  0x3,    0x16,   0x7,    0x16,   0x199,  0xa,    0x16,
      0xc,   0x16,  0xe,    0x16,   0x19c,  0xb,    0x16,   0x5,    0x16,
      0x19e, 0xa,   0x16,   0x3,    0x17,   0x3,    0x17,   0x3,    0x17,
      0x3,   0x17,  0x3,    0x17,   0x3,    0x17,   0x3,    0x17,   0x5,
      0x17,  0x1a7, 0xa,    0x17,   0x3,    0x18,   0x3,    0x18,   0x3,
      0x18,  0x3,   0x18,   0x3,    0x18,   0x3,    0x18,   0x3,    0x18,
      0x5,   0x18,  0x1b0,  0xa,    0x18,   0x3,    0x18,   0x7,    0x18,
      0x1b3, 0xa,   0x18,   0xc,    0x18,   0xe,    0x18,   0x1b6,  0xb,
      0x18,  0x3,   0x19,   0x3,    0x19,   0x3,    0x19,   0x3,    0x19,
      0x3,   0x1a,  0x3,    0x1a,   0x3,    0x1a,   0x3,    0x1a,   0x3,
      0x1b,  0x3,   0x1b,   0x5,    0x1b,   0x1c2,  0xa,    0x1b,   0x3,
      0x1b,  0x5,   0x1b,   0x1c5,  0xa,    0x1b,   0x3,    0x1c,   0x3,
      0x1c,  0x3,   0x1c,   0x3,    0x1c,   0x3,    0x1d,   0x3,    0x1d,
      0x5,   0x1d,  0x1cd,  0xa,    0x1d,   0x3,    0x1d,   0x3,    0x1d,
      0x5,   0x1d,  0x1d1,  0xa,    0x1d,   0x3,    0x1d,   0x7,    0x1d,
      0x1d4, 0xa,   0x1d,   0xc,    0x1d,   0xe,    0x1d,   0x1d7,  0xb,
      0x1d,  0x3,   0x1e,   0x3,    0x1e,   0x5,    0x1e,   0x1db,  0xa,
      0x1e,  0x3,   0x1e,   0x3,    0x1e,   0x5,    0x1e,   0x1df,  0xa,
      0x1e,  0x3,   0x1e,   0x3,    0x1e,   0x3,    0x1e,   0x5,    0x1e,
      0x1e4, 0xa,   0x1e,   0x3,    0x1f,   0x3,    0x1f,   0x3,    0x20,
      0x3,   0x20,  0x5,    0x20,   0x1ea,  0xa,    0x20,   0x3,    0x20,
      0x7,   0x20,  0x1ed,  0xa,    0x20,   0xc,    0x20,   0xe,    0x20,
      0x1f0, 0xb,   0x20,   0x3,    0x20,   0x3,    0x20,   0x3,    0x20,
      0x3,   0x20,  0x5,    0x20,   0x1f6,  0xa,    0x20,   0x3,    0x21,
      0x3,   0x21,  0x5,    0x21,   0x1fa,  0xa,    0x21,   0x3,    0x21,
      0x3,   0x21,  0x5,    0x21,   0x1fe,  0xa,    0x21,   0x5,    0x21,
      0x200, 0xa,   0x21,   0x3,    0x21,   0x3,    0x21,   0x5,    0x21,
      0x204, 0xa,   0x21,   0x5,    0x21,   0x206,  0xa,    0x21,   0x3,
      0x21,  0x3,   0x21,   0x5,    0x21,   0x20a,  0xa,    0x21,   0x5,
      0x21,  0x20c, 0xa,    0x21,   0x3,    0x21,   0x3,    0x21,   0x3,
      0x22,  0x3,   0x22,   0x5,    0x22,   0x212,  0xa,    0x22,   0x3,
      0x22,  0x3,   0x22,   0x3,    0x23,   0x3,    0x23,   0x5,    0x23,
      0x218, 0xa,   0x23,   0x3,    0x23,   0x3,    0x23,   0x5,    0x23,
      0x21c, 0xa,   0x23,   0x3,    0x23,   0x5,    0x23,   0x21f,  0xa,
      0x23,  0x3,   0x23,   0x5,    0x23,   0x222,  0xa,    0x23,   0x3,
      0x23,  0x3,   0x23,   0x5,    0x23,   0x226,  0xa,    0x23,   0x3,
      0x23,  0x3,   0x23,   0x3,    0x23,   0x3,    0x23,   0x5,    0x23,
      0x22c, 0xa,   0x23,   0x3,    0x23,   0x3,    0x23,   0x5,    0x23,
      0x230, 0xa,   0x23,   0x3,    0x23,   0x5,    0x23,   0x233,  0xa,
      0x23,  0x3,   0x23,   0x5,    0x23,   0x236,  0xa,    0x23,   0x3,
      0x23,  0x3,   0x23,   0x3,    0x23,   0x3,    0x23,   0x5,    0x23,
      0x23c, 0xa,   0x23,   0x3,    0x23,   0x5,    0x23,   0x23f,  0xa,
      0x23,  0x3,   0x23,   0x5,    0x23,   0x242,  0xa,    0x23,   0x3,
      0x23,  0x3,   0x23,   0x5,    0x23,   0x246,  0xa,    0x23,   0x3,
      0x23,  0x3,   0x23,   0x3,    0x23,   0x3,    0x23,   0x5,    0x23,
      0x24c, 0xa,   0x23,   0x3,    0x23,   0x5,    0x23,   0x24f,  0xa,
      0x23,  0x3,   0x23,   0x5,    0x23,   0x252,  0xa,    0x23,   0x3,
      0x23,  0x3,   0x23,   0x5,    0x23,   0x256,  0xa,    0x23,   0x3,
      0x24,  0x3,   0x24,   0x5,    0x24,   0x25a,  0xa,    0x24,   0x3,
      0x24,  0x5,   0x24,   0x25d,  0xa,    0x24,   0x3,    0x24,   0x5,
      0x24,  0x260, 0xa,    0x24,   0x3,    0x24,   0x5,    0x24,   0x263,
      0xa,   0x24,  0x3,    0x24,   0x5,    0x24,   0x266,  0xa,    0x24,
      0x3,   0x24,  0x3,    0x24,   0x3,    0x25,   0x3,    0x25,   0x5,
      0x25,  0x26c, 0xa,    0x25,   0x3,    0x26,   0x3,    0x26,   0x3,
      0x26,  0x5,   0x26,   0x271,  0xa,    0x26,   0x3,    0x26,   0x3,
      0x26,  0x5,   0x26,   0x275,  0xa,    0x26,   0x3,    0x26,   0x5,
      0x26,  0x278, 0xa,    0x26,   0x3,    0x26,   0x7,    0x26,   0x27b,
      0xa,   0x26,  0xc,    0x26,   0xe,    0x26,   0x27e,  0xb,    0x26,
      0x3,   0x27,  0x3,    0x27,   0x5,    0x27,   0x282,  0xa,    0x27,
      0x3,   0x27,  0x7,    0x27,   0x285,  0xa,    0x27,   0xc,    0x27,
      0xe,   0x27,  0x288,  0xb,    0x27,   0x3,    0x28,   0x3,    0x28,
      0x3,   0x28,  0x3,    0x29,   0x3,    0x29,   0x5,    0x29,   0x28f,
      0xa,   0x29,  0x3,    0x29,   0x3,    0x29,   0x5,    0x29,   0x293,
      0xa,   0x29,  0x5,    0x29,   0x295,  0xa,    0x29,   0x3,    0x29,
      0x3,   0x29,  0x5,    0x29,   0x299,  0xa,    0x29,   0x3,    0x29,
      0x3,   0x29,  0x5,    0x29,   0x29d,  0xa,    0x29,   0x5,    0x29,
      0x29f, 0xa,   0x29,   0x5,    0x29,   0x2a1,  0xa,    0x29,   0x3,
      0x2a,  0x3,   0x2a,   0x3,    0x2b,   0x3,    0x2b,   0x3,    0x2c,
      0x3,   0x2c,  0x3,    0x2d,   0x3,    0x2d,   0x3,    0x2d,   0x3,
      0x2d,  0x3,   0x2d,   0x7,    0x2d,   0x2ae,  0xa,    0x2d,   0xc,
      0x2d,  0xe,   0x2d,   0x2b1,  0xb,    0x2d,   0x3,    0x2e,   0x3,
      0x2e,  0x3,   0x2e,   0x3,    0x2e,   0x3,    0x2e,   0x7,    0x2e,
      0x2b8, 0xa,   0x2e,   0xc,    0x2e,   0xe,    0x2e,   0x2bb,  0xb,
      0x2e,  0x3,   0x2f,   0x3,    0x2f,   0x3,    0x2f,   0x3,    0x2f,
      0x3,   0x2f,  0x7,    0x2f,   0x2c2,  0xa,    0x2f,   0xc,    0x2f,
      0xe,   0x2f,  0x2c5,  0xb,    0x2f,   0x3,    0x30,   0x3,    0x30,
      0x5,   0x30,  0x2c9,  0xa,    0x30,   0x7,    0x30,   0x2cb,  0xa,
      0x30,  0xc,   0x30,   0xe,    0x30,   0x2ce,  0xb,    0x30,   0x3,
      0x30,  0x3,   0x30,   0x3,    0x31,   0x3,    0x31,   0x5,    0x31,
      0x2d4, 0xa,   0x31,   0x3,    0x31,   0x7,    0x31,   0x2d7,  0xa,
      0x31,  0xc,   0x31,   0xe,    0x31,   0x2da,  0xb,    0x31,   0x3,
      0x32,  0x3,   0x32,   0x5,    0x32,   0x2de,  0xa,    0x32,   0x3,
      0x32,  0x3,   0x32,   0x5,    0x32,   0x2e2,  0xa,    0x32,   0x3,
      0x32,  0x3,   0x32,   0x5,    0x32,   0x2e6,  0xa,    0x32,   0x3,
      0x32,  0x3,   0x32,   0x5,    0x32,   0x2ea,  0xa,    0x32,   0x3,
      0x32,  0x7,   0x32,   0x2ed,  0xa,    0x32,   0xc,    0x32,   0xe,
      0x32,  0x2f0, 0xb,    0x32,   0x3,    0x33,   0x3,    0x33,   0x5,
      0x33,  0x2f4, 0xa,    0x33,   0x3,    0x33,   0x3,    0x33,   0x5,
      0x33,  0x2f8, 0xa,    0x33,   0x3,    0x33,   0x3,    0x33,   0x5,
      0x33,  0x2fc, 0xa,    0x33,   0x3,    0x33,   0x3,    0x33,   0x5,
      0x33,  0x300, 0xa,    0x33,   0x3,    0x33,   0x3,    0x33,   0x5,
      0x33,  0x304, 0xa,    0x33,   0x3,    0x33,   0x3,    0x33,   0x5,
      0x33,  0x308, 0xa,    0x33,   0x3,    0x33,   0x7,    0x33,   0x30b,
      0xa,   0x33,  0xc,    0x33,   0xe,    0x33,   0x30e,  0xb,    0x33,
      0x3,   0x34,  0x3,    0x34,   0x5,    0x34,   0x312,  0xa,    0x34,
      0x3,   0x34,  0x3,    0x34,   0x5,    0x34,   0x316,  0xa,    0x34,
      0x3,   0x34,  0x7,    0x34,   0x319,  0xa,    0x34,   0xc,    0x34,
      0xe,   0x34,  0x31c,  0xb,    0x34,   0x3,    0x35,   0x3,    0x35,
      0x5,   0x35,  0x320,  0xa,    0x35,   0x7,    0x35,   0x322,  0xa,
      0x35,  0xc,   0x35,   0xe,    0x35,   0x325,  0xb,    0x35,   0x3,
      0x35,  0x3,   0x35,   0x3,    0x36,   0x3,    0x36,   0x5,    0x36,
      0x32b, 0xa,   0x36,   0x3,    0x36,   0x3,    0x36,   0x3,    0x36,
      0x3,   0x36,  0x3,    0x36,   0x5,    0x36,   0x332,  0xa,    0x36,
      0x3,   0x36,  0x3,    0x36,   0x5,    0x36,   0x336,  0xa,    0x36,
      0x3,   0x36,  0x3,    0x36,   0x5,    0x36,   0x33a,  0xa,    0x36,
      0x3,   0x36,  0x3,    0x36,   0x5,    0x36,   0x33e,  0xa,    0x36,
      0x3,   0x36,  0x3,    0x36,   0x3,    0x36,   0x3,    0x36,   0x3,
      0x36,  0x3,   0x36,   0x3,    0x36,   0x3,    0x36,   0x3,    0x36,
      0x3,   0x36,  0x3,    0x36,   0x3,    0x36,   0x3,    0x36,   0x5,
      0x36,  0x34d, 0xa,    0x36,   0x3,    0x36,   0x5,    0x36,   0x350,
      0xa,   0x36,  0x3,    0x36,   0x3,    0x36,   0x3,    0x36,   0x3,
      0x36,  0x3,   0x36,   0x3,    0x36,   0x3,    0x36,   0x3,    0x36,
      0x3,   0x36,  0x3,    0x36,   0x3,    0x36,   0x7,    0x36,   0x35d,
      0xa,   0x36,  0xc,    0x36,   0xe,    0x36,   0x360,  0xb,    0x36,
      0x3,   0x37,  0x3,    0x37,   0x3,    0x37,   0x7,    0x37,   0x365,
      0xa,   0x37,  0xc,    0x37,   0xe,    0x37,   0x368,  0xb,    0x37,
      0x3,   0x38,  0x3,    0x38,   0x3,    0x38,   0x3,    0x38,   0x5,
      0x38,  0x36e, 0xa,    0x38,   0x3,    0x38,   0x3,    0x38,   0x5,
      0x38,  0x372, 0xa,    0x38,   0x3,    0x38,   0x3,    0x38,   0x5,
      0x38,  0x376, 0xa,    0x38,   0x3,    0x38,   0x3,    0x38,   0x3,
      0x38,  0x3,   0x38,   0x5,    0x38,   0x37c,  0xa,    0x38,   0x3,
      0x38,  0x3,   0x38,   0x5,    0x38,   0x380,  0xa,    0x38,   0x3,
      0x38,  0x3,   0x38,   0x5,    0x38,   0x384,  0xa,    0x38,   0x3,
      0x38,  0x3,   0x38,   0x3,    0x38,   0x3,    0x38,   0x5,    0x38,
      0x38a, 0xa,   0x38,   0x3,    0x38,   0x3,    0x38,   0x5,    0x38,
      0x38e, 0xa,   0x38,   0x3,    0x38,   0x3,    0x38,   0x5,    0x38,
      0x392, 0xa,   0x38,   0x3,    0x38,   0x5,    0x38,   0x395,  0xa,
      0x38,  0x3,   0x38,   0x3,    0x38,   0x5,    0x38,   0x399,  0xa,
      0x38,  0x3,   0x38,   0x3,    0x38,   0x3,    0x38,   0x3,    0x38,
      0x5,   0x38,  0x39f,  0xa,    0x38,   0x3,    0x38,   0x3,    0x38,
      0x5,   0x38,  0x3a3,  0xa,    0x38,   0x3,    0x38,   0x3,    0x38,
      0x5,   0x38,  0x3a7,  0xa,    0x38,   0x3,    0x38,   0x3,    0x38,
      0x3,   0x38,  0x3,    0x38,   0x5,    0x38,   0x3ad,  0xa,    0x38,
      0x3,   0x38,  0x3,    0x38,   0x5,    0x38,   0x3b1,  0xa,    0x38,
      0x3,   0x38,  0x3,    0x38,   0x5,    0x38,   0x3b5,  0xa,    0x38,
      0x3,   0x38,  0x3,    0x38,   0x3,    0x38,   0x3,    0x38,   0x5,
      0x38,  0x3bb, 0xa,    0x38,   0x3,    0x38,   0x3,    0x38,   0x5,
      0x38,  0x3bf, 0xa,    0x38,   0x3,    0x38,   0x3,    0x38,   0x5,
      0x38,  0x3c3, 0xa,    0x38,   0x3,    0x38,   0x3,    0x38,   0x3,
      0x38,  0x3,   0x38,   0x5,    0x38,   0x3c9,  0xa,    0x38,   0x3,
      0x38,  0x3,   0x38,   0x5,    0x38,   0x3cd,  0xa,    0x38,   0x3,
      0x38,  0x3,   0x38,   0x5,    0x38,   0x3d1,  0xa,    0x38,   0x3,
      0x38,  0x3,   0x38,   0x3,    0x38,   0x3,    0x38,   0x3,    0x38,
      0x3,   0x38,  0x5,    0x38,   0x3d9,  0xa,    0x38,   0x3,    0x39,
      0x3,   0x39,  0x3,    0x39,   0x3,    0x39,   0x3,    0x39,   0x3,
      0x39,  0x5,   0x39,   0x3e1,  0xa,    0x39,   0x3,    0x3a,   0x3,
      0x3a,  0x3,   0x3b,   0x3,    0x3b,   0x5,    0x3b,   0x3e7,  0xa,
      0x3b,  0x3,   0x3b,   0x3,    0x3b,   0x5,    0x3b,   0x3eb,  0xa,
      0x3b,  0x3,   0x3b,   0x3,    0x3b,   0x5,    0x3b,   0x3ef,  0xa,
      0x3b,  0x3,   0x3b,   0x3,    0x3b,   0x5,    0x3b,   0x3f3,  0xa,
      0x3b,  0x7,   0x3b,   0x3f5,  0xa,    0x3b,   0xc,    0x3b,   0xe,
      0x3b,  0x3f8, 0xb,    0x3b,   0x5,    0x3b,   0x3fa,  0xa,    0x3b,
      0x3,   0x3b,  0x3,    0x3b,   0x3,    0x3c,   0x3,    0x3c,   0x5,
      0x3c,  0x400, 0xa,    0x3c,   0x3,    0x3c,   0x3,    0x3c,   0x3,
      0x3c,  0x5,   0x3c,   0x405,  0xa,    0x3c,   0x3,    0x3c,   0x3,
      0x3c,  0x3,   0x3c,   0x5,    0x3c,   0x40a,  0xa,    0x3c,   0x3,
      0x3c,  0x3,   0x3c,   0x3,    0x3c,   0x5,    0x3c,   0x40f,  0xa,
      0x3c,  0x3,   0x3c,   0x3,    0x3c,   0x3,    0x3c,   0x5,    0x3c,
      0x414, 0xa,   0x3c,   0x3,    0x3c,   0x3,    0x3c,   0x3,    0x3c,
      0x5,   0x3c,  0x419,  0xa,    0x3c,   0x3,    0x3c,   0x3,    0x3c,
      0x3,   0x3c,  0x5,    0x3c,   0x41e,  0xa,    0x3c,   0x3,    0x3c,
      0x5,   0x3c,  0x421,  0xa,    0x3c,   0x3,    0x3d,   0x3,    0x3d,
      0x5,   0x3d,  0x425,  0xa,    0x3d,   0x3,    0x3d,   0x3,    0x3d,
      0x5,   0x3d,  0x429,  0xa,    0x3d,   0x3,    0x3d,   0x3,    0x3d,
      0x3,   0x3e,  0x3,    0x3e,   0x5,    0x3e,   0x42f,  0xa,    0x3e,
      0x3,   0x3e,  0x6,    0x3e,   0x432,  0xa,    0x3e,   0xd,    0x3e,
      0xe,   0x3e,  0x433,  0x3,    0x3f,   0x3,    0x3f,   0x5,    0x3f,
      0x438, 0xa,   0x3f,   0x3,    0x3f,   0x5,    0x3f,   0x43b,  0xa,
      0x3f,  0x3,   0x40,   0x3,    0x40,   0x3,    0x40,   0x3,    0x40,
      0x3,   0x40,  0x3,    0x40,   0x3,    0x41,   0x3,    0x41,   0x5,
      0x41,  0x445, 0xa,    0x41,   0x3,    0x41,   0x3,    0x41,   0x5,
      0x41,  0x449, 0xa,    0x41,   0x3,    0x41,   0x3,    0x41,   0x5,
      0x41,  0x44d, 0xa,    0x41,   0x5,    0x41,   0x44f,  0xa,    0x41,
      0x3,   0x41,  0x3,    0x41,   0x5,    0x41,   0x453,  0xa,    0x41,
      0x3,   0x41,  0x3,    0x41,   0x5,    0x41,   0x457,  0xa,    0x41,
      0x3,   0x41,  0x3,    0x41,   0x5,    0x41,   0x45b,  0xa,    0x41,
      0x7,   0x41,  0x45d,  0xa,    0x41,   0xc,    0x41,   0xe,    0x41,
      0x460, 0xb,   0x41,   0x5,    0x41,   0x462,  0xa,    0x41,   0x3,
      0x41,  0x3,   0x41,   0x3,    0x42,   0x3,    0x42,   0x3,    0x43,
      0x3,   0x43,  0x5,    0x43,   0x46a,  0xa,    0x43,   0x3,    0x43,
      0x3,   0x43,  0x5,    0x43,   0x46e,  0xa,    0x43,   0x3,    0x43,
      0x3,   0x43,  0x5,    0x43,   0x472,  0xa,    0x43,   0x3,    0x43,
      0x5,   0x43,  0x475,  0xa,    0x43,   0x3,    0x43,   0x5,    0x43,
      0x478, 0xa,   0x43,   0x3,    0x43,   0x3,    0x43,   0x3,    0x44,
      0x5,   0x44,  0x47d,  0xa,    0x44,   0x3,    0x44,   0x3,    0x44,
      0x5,   0x44,  0x481,  0xa,    0x44,   0x3,    0x44,   0x3,    0x44,
      0x3,   0x44,  0x3,    0x44,   0x5,    0x44,   0x487,  0xa,    0x44,
      0x3,   0x45,  0x3,    0x45,   0x3,    0x46,   0x3,    0x46,   0x5,
      0x46,  0x48d, 0xa,    0x46,   0x3,    0x47,   0x3,    0x47,   0x5,
      0x47,  0x491, 0xa,    0x47,   0x3,    0x47,   0x3,    0x47,   0x5,
      0x47,  0x495, 0xa,    0x47,   0x3,    0x47,   0x3,    0x47,   0x5,
      0x47,  0x499, 0xa,    0x47,   0x3,    0x47,   0x3,    0x47,   0x5,
      0x47,  0x49d, 0xa,    0x47,   0x3,    0x47,   0x3,    0x47,   0x5,
      0x47,  0x4a1, 0xa,    0x47,   0x3,    0x47,   0x3,    0x47,   0x5,
      0x47,  0x4a5, 0xa,    0x47,   0x3,    0x47,   0x3,    0x47,   0x5,
      0x47,  0x4a9, 0xa,    0x47,   0x3,    0x47,   0x3,    0x47,   0x5,
      0x47,  0x4ad, 0xa,    0x47,   0x7,    0x47,   0x4af,  0xa,    0x47,
      0xc,   0x47,  0xe,    0x47,   0x4b2,  0xb,    0x47,   0x5,    0x47,
      0x4b4, 0xa,   0x47,   0x3,    0x47,   0x3,    0x47,   0x3,    0x48,
      0x3,   0x48,  0x3,    0x48,   0x5,    0x48,   0x4bb,  0xa,    0x48,
      0x3,   0x49,  0x3,    0x49,   0x5,    0x49,   0x4bf,  0xa,    0x49,
      0x3,   0x49,  0x6,    0x49,   0x4c2,  0xa,    0x49,   0xd,    0x49,
      0xe,   0x49,  0x4c3,  0x3,    0x4a,   0x3,    0x4a,   0x3,    0x4b,
      0x3,   0x4b,  0x3,    0x4c,   0x3,    0x4c,   0x3,    0x4d,   0x3,
      0x4d,  0x3,   0x4e,   0x3,    0x4e,   0x3,    0x4f,   0x3,    0x4f,
      0x3,   0x50,  0x3,    0x50,   0x3,    0x50,   0x2,    0x2,    0x51,
      0x2,   0x4,   0x6,    0x8,    0xa,    0xc,    0xe,    0x10,   0x12,
      0x14,  0x16,  0x18,   0x1a,   0x1c,   0x1e,   0x20,   0x22,   0x24,
      0x26,  0x28,  0x2a,   0x2c,   0x2e,   0x30,   0x32,   0x34,   0x36,
      0x38,  0x3a,  0x3c,   0x3e,   0x40,   0x42,   0x44,   0x46,   0x48,
      0x4a,  0x4c,  0x4e,   0x50,   0x52,   0x54,   0x56,   0x58,   0x5a,
      0x5c,  0x5e,  0x60,   0x62,   0x64,   0x66,   0x68,   0x6a,   0x6c,
      0x6e,  0x70,  0x72,   0x74,   0x76,   0x78,   0x7a,   0x7c,   0x7e,
      0x80,  0x82,  0x84,   0x86,   0x88,   0x8a,   0x8c,   0x8e,   0x90,
      0x92,  0x94,  0x96,   0x98,   0x9a,   0x9c,   0x9e,   0x2,    0xd,
      0x3,   0x2,   0x56,   0x59,   0x3,    0x2,    0x10,   0x11,   0x3,
      0x2,   0x6b,  0x6c,   0x5,    0x2,    0x65,   0x65,   0x6d,   0x6d,
      0x70,  0x70,  0x4,    0x2,    0xb,    0xb,    0x1d,   0x1d,   0x3,
      0x2,   0x36,  0x38,   0x3,    0x2,    0x40,   0x41,   0x5,    0x2,
      0x39,  0x39,  0x42,   0x6d,   0x70,   0x70,   0x4,    0x2,    0x18,
      0x18,  0x21,  0x24,   0x4,    0x2,    0x19,   0x19,   0x25,   0x28,
      0x4,   0x2,   0x11,   0x11,   0x29,   0x33,   0x586,  0x2,    0xa1,
      0x3,   0x2,   0x2,    0x2,    0x4,    0xad,   0x3,    0x2,    0x2,
      0x2,   0x6,   0xaf,   0x3,    0x2,    0x2,    0x2,    0x8,    0xb1,
      0x3,   0x2,   0x2,    0x2,    0xa,    0xbb,   0x3,    0x2,    0x2,
      0x2,   0xc,   0xd1,   0x3,    0x2,    0x2,    0x2,    0xe,    0xdc,
      0x3,   0x2,   0x2,    0x2,    0x10,   0xe0,   0x3,    0x2,    0x2,
      0x2,   0x12,  0xed,   0x3,    0x2,    0x2,    0x2,    0x14,   0xf7,
      0x3,   0x2,   0x2,    0x2,    0x16,   0x10d,  0x3,    0x2,    0x2,
      0x2,   0x18,  0x10f,  0x3,    0x2,    0x2,    0x2,    0x1a,   0x115,
      0x3,   0x2,   0x2,    0x2,    0x1c,   0x12d,  0x3,    0x2,    0x2,
      0x2,   0x1e,  0x131,  0x3,    0x2,    0x2,    0x2,    0x20,   0x145,
      0x3,   0x2,   0x2,    0x2,    0x22,   0x159,  0x3,    0x2,    0x2,
      0x2,   0x24,  0x15b,  0x3,    0x2,    0x2,    0x2,    0x26,   0x16a,
      0x3,   0x2,   0x2,    0x2,    0x28,   0x174,  0x3,    0x2,    0x2,
      0x2,   0x2a,  0x19d,  0x3,    0x2,    0x2,    0x2,    0x2c,   0x1a6,
      0x3,   0x2,   0x2,    0x2,    0x2e,   0x1a8,  0x3,    0x2,    0x2,
      0x2,   0x30,  0x1b7,  0x3,    0x2,    0x2,    0x2,    0x32,   0x1bb,
      0x3,   0x2,   0x2,    0x2,    0x34,   0x1bf,  0x3,    0x2,    0x2,
      0x2,   0x36,  0x1c6,  0x3,    0x2,    0x2,    0x2,    0x38,   0x1ca,
      0x3,   0x2,   0x2,    0x2,    0x3a,   0x1e3,  0x3,    0x2,    0x2,
      0x2,   0x3c,  0x1e5,  0x3,    0x2,    0x2,    0x2,    0x3e,   0x1f5,
      0x3,   0x2,   0x2,    0x2,    0x40,   0x1f7,  0x3,    0x2,    0x2,
      0x2,   0x42,  0x20f,  0x3,    0x2,    0x2,    0x2,    0x44,   0x255,
      0x3,   0x2,   0x2,    0x2,    0x46,   0x257,  0x3,    0x2,    0x2,
      0x2,   0x48,  0x26b,  0x3,    0x2,    0x2,    0x2,    0x4a,   0x26d,
      0x3,   0x2,   0x2,    0x2,    0x4c,   0x27f,  0x3,    0x2,    0x2,
      0x2,   0x4e,  0x289,  0x3,    0x2,    0x2,    0x2,    0x50,   0x28c,
      0x3,   0x2,   0x2,    0x2,    0x52,   0x2a2,  0x3,    0x2,    0x2,
      0x2,   0x54,  0x2a4,  0x3,    0x2,    0x2,    0x2,    0x56,   0x2a6,
      0x3,   0x2,   0x2,    0x2,    0x58,   0x2a8,  0x3,    0x2,    0x2,
      0x2,   0x5a,  0x2b2,  0x3,    0x2,    0x2,    0x2,    0x5c,   0x2bc,
      0x3,   0x2,   0x2,    0x2,    0x5e,   0x2cc,  0x3,    0x2,    0x2,
      0x2,   0x60,  0x2d1,  0x3,    0x2,    0x2,    0x2,    0x62,   0x2db,
      0x3,   0x2,   0x2,    0x2,    0x64,   0x2f1,  0x3,    0x2,    0x2,
      0x2,   0x66,  0x30f,  0x3,    0x2,    0x2,    0x2,    0x68,   0x323,
      0x3,   0x2,   0x2,    0x2,    0x6a,   0x328,  0x3,    0x2,    0x2,
      0x2,   0x6c,  0x361,  0x3,    0x2,    0x2,    0x2,    0x6e,   0x3d8,
      0x3,   0x2,   0x2,    0x2,    0x70,   0x3e0,  0x3,    0x2,    0x2,
      0x2,   0x72,  0x3e2,  0x3,    0x2,    0x2,    0x2,    0x74,   0x3e4,
      0x3,   0x2,   0x2,    0x2,    0x76,   0x420,  0x3,    0x2,    0x2,
      0x2,   0x78,  0x422,  0x3,    0x2,    0x2,    0x2,    0x7a,   0x42c,
      0x3,   0x2,   0x2,    0x2,    0x7c,   0x435,  0x3,    0x2,    0x2,
      0x2,   0x7e,  0x43c,  0x3,    0x2,    0x2,    0x2,    0x80,   0x442,
      0x3,   0x2,   0x2,    0x2,    0x82,   0x465,  0x3,    0x2,    0x2,
      0x2,   0x84,  0x467,  0x3,    0x2,    0x2,    0x2,    0x86,   0x47c,
      0x3,   0x2,   0x2,    0x2,    0x88,   0x488,  0x3,    0x2,    0x2,
      0x2,   0x8a,  0x48c,  0x3,    0x2,    0x2,    0x2,    0x8c,   0x48e,
      0x3,   0x2,   0x2,    0x2,    0x8e,   0x4b7,  0x3,    0x2,    0x2,
      0x2,   0x90,  0x4bc,  0x3,    0x2,    0x2,    0x2,    0x92,   0x4c5,
      0x3,   0x2,   0x2,    0x2,    0x94,   0x4c7,  0x3,    0x2,    0x2,
      0x2,   0x96,  0x4c9,  0x3,    0x2,    0x2,    0x2,    0x98,   0x4cb,
      0x3,   0x2,   0x2,    0x2,    0x9a,   0x4cd,  0x3,    0x2,    0x2,
      0x2,   0x9c,  0x4cf,  0x3,    0x2,    0x2,    0x2,    0x9e,   0x4d1,
      0x3,   0x2,   0x2,    0x2,    0xa0,   0xa2,   0x7,    0x71,   0x2,
      0x2,   0xa1,  0xa0,   0x3,    0x2,    0x2,    0x2,    0xa1,   0xa2,
      0x3,   0x2,   0x2,    0x2,    0xa2,   0xa3,   0x3,    0x2,    0x2,
      0x2,   0xa3,  0xa8,   0x5,    0x4,    0x3,    0x2,    0xa4,   0xa6,
      0x7,   0x71,  0x2,    0x2,    0xa5,   0xa4,   0x3,    0x2,    0x2,
      0x2,   0xa5,  0xa6,   0x3,    0x2,    0x2,    0x2,    0xa6,   0xa7,
      0x3,   0x2,   0x2,    0x2,    0xa7,   0xa9,   0x7,    0x3,    0x2,
      0x2,   0xa8,  0xa5,   0x3,    0x2,    0x2,    0x2,    0xa8,   0xa9,
      0x3,   0x2,   0x2,    0x2,    0xa9,   0xab,   0x3,    0x2,    0x2,
      0x2,   0xaa,  0xac,   0x7,    0x71,   0x2,    0x2,    0xab,   0xaa,
      0x3,   0x2,   0x2,    0x2,    0xab,   0xac,   0x3,    0x2,    0x2,
      0x2,   0xac,  0x3,    0x3,    0x2,    0x2,    0x2,    0xad,   0xae,
      0x5,   0x6,   0x4,    0x2,    0xae,   0x5,    0x3,    0x2,    0x2,
      0x2,   0xaf,  0xb0,   0x5,    0x8,    0x5,    0x2,    0xb0,   0x7,
      0x3,   0x2,   0x2,    0x2,    0xb1,   0xb8,   0x5,    0xa,    0x6,
      0x2,   0xb2,  0xb4,   0x7,    0x71,   0x2,    0x2,    0xb3,   0xb2,
      0x3,   0x2,   0x2,    0x2,    0xb3,   0xb4,   0x3,    0x2,    0x2,
      0x2,   0xb4,  0xb5,   0x3,    0x2,    0x2,    0x2,    0xb5,   0xb7,
      0x5,   0xc,   0x7,    0x2,    0xb6,   0xb3,   0x3,    0x2,    0x2,
      0x2,   0xb7,  0xba,   0x3,    0x2,    0x2,    0x2,    0xb8,   0xb6,
      0x3,   0x2,   0x2,    0x2,    0xb8,   0xb9,   0x3,    0x2,    0x2,
      0x2,   0xb9,  0x9,    0x3,    0x2,    0x2,    0x2,    0xba,   0xb8,
      0x3,   0x2,   0x2,    0x2,    0xbb,   0xc2,   0x5,    0xe,    0x8,
      0x2,   0xbc,  0xbe,   0x7,    0x71,   0x2,    0x2,    0xbd,   0xbc,
      0x3,   0x2,   0x2,    0x2,    0xbd,   0xbe,   0x3,    0x2,    0x2,
      0x2,   0xbe,  0xbf,   0x3,    0x2,    0x2,    0x2,    0xbf,   0xc1,
      0x5,   0xe,   0x8,    0x2,    0xc0,   0xbd,   0x3,    0x2,    0x2,
      0x2,   0xc1,  0xc4,   0x3,    0x2,    0x2,    0x2,    0xc2,   0xc0,
      0x3,   0x2,   0x2,    0x2,    0xc2,   0xc3,   0x3,    0x2,    0x2,
      0x2,   0xc3,  0xb,    0x3,    0x2,    0x2,    0x2,    0xc4,   0xc2,
      0x3,   0x2,   0x2,    0x2,    0xc5,   0xc6,   0x7,    0x42,   0x2,
      0x2,   0xc6,  0xc7,   0x7,    0x71,   0x2,    0x2,    0xc7,   0xc9,
      0x7,   0x43,  0x2,    0x2,    0xc8,   0xca,   0x7,    0x71,   0x2,
      0x2,   0xc9,  0xc8,   0x3,    0x2,    0x2,    0x2,    0xc9,   0xca,
      0x3,   0x2,   0x2,    0x2,    0xca,   0xcb,   0x3,    0x2,    0x2,
      0x2,   0xcb,  0xd2,   0x5,    0xa,    0x6,    0x2,    0xcc,   0xce,
      0x7,   0x42,  0x2,    0x2,    0xcd,   0xcf,   0x7,    0x71,   0x2,
      0x2,   0xce,  0xcd,   0x3,    0x2,    0x2,    0x2,    0xce,   0xcf,
      0x3,   0x2,   0x2,    0x2,    0xcf,   0xd0,   0x3,    0x2,    0x2,
      0x2,   0xd0,  0xd2,   0x5,    0xa,    0x6,    0x2,    0xd1,   0xc5,
      0x3,   0x2,   0x2,    0x2,    0xd1,   0xcc,   0x3,    0x2,    0x2,
      0x2,   0xd2,  0xd,    0x3,    0x2,    0x2,    0x2,    0xd3,   0xdd,
      0x5,   0x10,  0x9,    0x2,    0xd4,   0xdd,   0x5,    0x12,   0xa,
      0x2,   0xd5,  0xdd,   0x5,    0x14,   0xb,    0x2,    0xd6,   0xdd,
      0x5,   0x18,  0xd,    0x2,    0xd7,   0xdd,   0x5,    0x1a,   0xe,
      0x2,   0xd8,  0xdd,   0x5,    0x1e,   0x10,   0x2,    0xd9,   0xdd,
      0x5,   0x20,  0x11,   0x2,    0xda,   0xdd,   0x5,    0x24,   0x13,
      0x2,   0xdb,  0xdd,   0x5,    0x26,   0x14,   0x2,    0xdc,   0xd3,
      0x3,   0x2,   0x2,    0x2,    0xdc,   0xd4,   0x3,    0x2,    0x2,
      0x2,   0xdc,  0xd5,   0x3,    0x2,    0x2,    0x2,    0xdc,   0xd6,
      0x3,   0x2,   0x2,    0x2,    0xdc,   0xd7,   0x3,    0x2,    0x2,
      0x2,   0xdc,  0xd8,   0x3,    0x2,    0x2,    0x2,    0xdc,   0xd9,
      0x3,   0x2,   0x2,    0x2,    0xdc,   0xda,   0x3,    0x2,    0x2,
      0x2,   0xdc,  0xdb,   0x3,    0x2,    0x2,    0x2,    0xdd,   0xf,
      0x3,   0x2,   0x2,    0x2,    0xde,   0xdf,   0x7,    0x44,   0x2,
      0x2,   0xdf,  0xe1,   0x7,    0x71,   0x2,    0x2,    0xe0,   0xde,
      0x3,   0x2,   0x2,    0x2,    0xe0,   0xe1,   0x3,    0x2,    0x2,
      0x2,   0xe1,  0xe2,   0x3,    0x2,    0x2,    0x2,    0xe2,   0xe4,
      0x7,   0x45,  0x2,    0x2,    0xe3,   0xe5,   0x7,    0x71,   0x2,
      0x2,   0xe4,  0xe3,   0x3,    0x2,    0x2,    0x2,    0xe4,   0xe5,
      0x3,   0x2,   0x2,    0x2,    0xe5,   0xe6,   0x3,    0x2,    0x2,
      0x2,   0xe6,  0xeb,   0x5,    0x38,   0x1d,   0x2,    0xe7,   0xe9,
      0x7,   0x71,  0x2,    0x2,    0xe8,   0xe7,   0x3,    0x2,    0x2,
      0x2,   0xe8,  0xe9,   0x3,    0x2,    0x2,    0x2,    0xe9,   0xea,
      0x3,   0x2,   0x2,    0x2,    0xea,   0xec,   0x5,    0x36,   0x1c,
      0x2,   0xeb,  0xe8,   0x3,    0x2,    0x2,    0x2,    0xeb,   0xec,
      0x3,   0x2,   0x2,    0x2,    0xec,   0x11,   0x3,    0x2,    0x2,
      0x2,   0xed,  0xef,   0x7,    0x46,   0x2,    0x2,    0xee,   0xf0,
      0x7,   0x71,  0x2,    0x2,    0xef,   0xee,   0x3,    0x2,    0x2,
      0x2,   0xef,  0xf0,   0x3,    0x2,    0x2,    0x2,    0xf0,   0xf1,
      0x3,   0x2,   0x2,    0x2,    0xf1,   0xf2,   0x5,    0x56,   0x2c,
      0x2,   0xf2,  0xf3,   0x7,    0x71,   0x2,    0x2,    0xf3,   0xf4,
      0x7,   0x47,  0x2,    0x2,    0xf4,   0xf5,   0x7,    0x71,   0x2,
      0x2,   0xf5,  0xf6,   0x5,    0x88,   0x45,   0x2,    0xf6,   0x13,
      0x3,   0x2,   0x2,    0x2,    0xf7,   0xf9,   0x7,    0x48,   0x2,
      0x2,   0xf8,  0xfa,   0x7,    0x71,   0x2,    0x2,    0xf9,   0xf8,
      0x3,   0x2,   0x2,    0x2,    0xf9,   0xfa,   0x3,    0x2,    0x2,
      0x2,   0xfa,  0xfb,   0x3,    0x2,    0x2,    0x2,    0xfb,   0x100,
      0x5,   0x3a,  0x1e,   0x2,    0xfc,   0xfd,   0x7,    0x71,   0x2,
      0x2,   0xfd,  0xff,   0x5,    0x16,   0xc,    0x2,    0xfe,   0xfc,
      0x3,   0x2,   0x2,    0x2,    0xff,   0x102,  0x3,    0x2,    0x2,
      0x2,   0x100, 0xfe,   0x3,    0x2,    0x2,    0x2,    0x100,  0x101,
      0x3,   0x2,   0x2,    0x2,    0x101,  0x15,   0x3,    0x2,    0x2,
      0x2,   0x102, 0x100,  0x3,    0x2,    0x2,    0x2,    0x103,  0x104,
      0x7,   0x49,  0x2,    0x2,    0x104,  0x105,  0x7,    0x71,   0x2,
      0x2,   0x105, 0x106,  0x7,    0x45,   0x2,    0x2,    0x106,  0x107,
      0x7,   0x71,  0x2,    0x2,    0x107,  0x10e,  0x5,    0x1a,   0xe,
      0x2,   0x108, 0x109,  0x7,    0x49,   0x2,    0x2,    0x109,  0x10a,
      0x7,   0x71,  0x2,    0x2,    0x10a,  0x10b,  0x7,    0x4a,   0x2,
      0x2,   0x10b, 0x10c,  0x7,    0x71,   0x2,    0x2,    0x10c,  0x10e,
      0x5,   0x1a,  0xe,    0x2,    0x10d,  0x103,  0x3,    0x2,    0x2,
      0x2,   0x10d, 0x108,  0x3,    0x2,    0x2,    0x2,    0x10e,  0x17,
      0x3,   0x2,   0x2,    0x2,    0x10f,  0x111,  0x7,    0x4a,   0x2,
      0x2,   0x110, 0x112,  0x7,    0x71,   0x2,    0x2,    0x111,  0x110,
      0x3,   0x2,   0x2,    0x2,    0x111,  0x112,  0x3,    0x2,    0x2,
      0x2,   0x112, 0x113,  0x3,    0x2,    0x2,    0x2,    0x113,  0x114,
      0x5,   0x38,  0x1d,   0x2,    0x114,  0x19,   0x3,    0x2,    0x2,
      0x2,   0x115, 0x116,  0x7,    0x4b,   0x2,    0x2,    0x116,  0x11b,
      0x5,   0x1c,  0xf,    0x2,    0x117,  0x118,  0x7,    0x4,    0x2,
      0x2,   0x118, 0x11a,  0x5,    0x1c,   0xf,    0x2,    0x119,  0x117,
      0x3,   0x2,   0x2,    0x2,    0x11a,  0x11d,  0x3,    0x2,    0x2,
      0x2,   0x11b, 0x119,  0x3,    0x2,    0x2,    0x2,    0x11b,  0x11c,
      0x3,   0x2,   0x2,    0x2,    0x11c,  0x1b,   0x3,    0x2,    0x2,
      0x2,   0x11d, 0x11b,  0x3,    0x2,    0x2,    0x2,    0x11e,  0x11f,
      0x5,   0x90,  0x49,   0x2,    0x11f,  0x120,  0x7,    0x5,    0x2,
      0x2,   0x120, 0x121,  0x5,    0x56,   0x2c,   0x2,    0x121,  0x12e,
      0x3,   0x2,   0x2,    0x2,    0x122,  0x123,  0x5,    0x88,   0x45,
      0x2,   0x123, 0x124,  0x7,    0x5,    0x2,    0x2,    0x124,  0x125,
      0x5,   0x56,  0x2c,   0x2,    0x125,  0x12e,  0x3,    0x2,    0x2,
      0x2,   0x126, 0x127,  0x5,    0x88,   0x45,   0x2,    0x127,  0x128,
      0x7,   0x6,   0x2,    0x2,    0x128,  0x129,  0x5,    0x56,   0x2c,
      0x2,   0x129, 0x12e,  0x3,    0x2,    0x2,    0x2,    0x12a,  0x12b,
      0x5,   0x88,  0x45,   0x2,    0x12b,  0x12c,  0x5,    0x4c,   0x27,
      0x2,   0x12c, 0x12e,  0x3,    0x2,    0x2,    0x2,    0x12d,  0x11e,
      0x3,   0x2,   0x2,    0x2,    0x12d,  0x122,  0x3,    0x2,    0x2,
      0x2,   0x12d, 0x126,  0x3,    0x2,    0x2,    0x2,    0x12d,  0x12a,
      0x3,   0x2,   0x2,    0x2,    0x12e,  0x1d,   0x3,    0x2,    0x2,
      0x2,   0x12f, 0x130,  0x7,    0x4c,   0x2,    0x2,    0x130,  0x132,
      0x7,   0x71,  0x2,    0x2,    0x131,  0x12f,  0x3,    0x2,    0x2,
      0x2,   0x131, 0x132,  0x3,    0x2,    0x2,    0x2,    0x132,  0x133,
      0x3,   0x2,   0x2,    0x2,    0x133,  0x135,  0x7,    0x4d,   0x2,
      0x2,   0x134, 0x136,  0x7,    0x71,   0x2,    0x2,    0x135,  0x134,
      0x3,   0x2,   0x2,    0x2,    0x135,  0x136,  0x3,    0x2,    0x2,
      0x2,   0x136, 0x137,  0x3,    0x2,    0x2,    0x2,    0x137,  0x142,
      0x5,   0x56,  0x2c,   0x2,    0x138,  0x13a,  0x7,    0x71,   0x2,
      0x2,   0x139, 0x138,  0x3,    0x2,    0x2,    0x2,    0x139,  0x13a,
      0x3,   0x2,   0x2,    0x2,    0x13a,  0x13b,  0x3,    0x2,    0x2,
      0x2,   0x13b, 0x13d,  0x7,    0x4,    0x2,    0x2,    0x13c,  0x13e,
      0x7,   0x71,  0x2,    0x2,    0x13d,  0x13c,  0x3,    0x2,    0x2,
      0x2,   0x13d, 0x13e,  0x3,    0x2,    0x2,    0x2,    0x13e,  0x13f,
      0x3,   0x2,   0x2,    0x2,    0x13f,  0x141,  0x5,    0x56,   0x2c,
      0x2,   0x140, 0x139,  0x3,    0x2,    0x2,    0x2,    0x141,  0x144,
      0x3,   0x2,   0x2,    0x2,    0x142,  0x140,  0x3,    0x2,    0x2,
      0x2,   0x142, 0x143,  0x3,    0x2,    0x2,    0x2,    0x143,  0x1f,
      0x3,   0x2,   0x2,    0x2,    0x144,  0x142,  0x3,    0x2,    0x2,
      0x2,   0x145, 0x146,  0x7,    0x4e,   0x2,    0x2,    0x146,  0x147,
      0x7,   0x71,  0x2,    0x2,    0x147,  0x152,  0x5,    0x22,   0x12,
      0x2,   0x148, 0x14a,  0x7,    0x71,   0x2,    0x2,    0x149,  0x148,
      0x3,   0x2,   0x2,    0x2,    0x149,  0x14a,  0x3,    0x2,    0x2,
      0x2,   0x14a, 0x14b,  0x3,    0x2,    0x2,    0x2,    0x14b,  0x14d,
      0x7,   0x4,   0x2,    0x2,    0x14c,  0x14e,  0x7,    0x71,   0x2,
      0x2,   0x14d, 0x14c,  0x3,    0x2,    0x2,    0x2,    0x14d,  0x14e,
      0x3,   0x2,   0x2,    0x2,    0x14e,  0x14f,  0x3,    0x2,    0x2,
      0x2,   0x14f, 0x151,  0x5,    0x22,   0x12,   0x2,    0x150,  0x149,
      0x3,   0x2,   0x2,    0x2,    0x151,  0x154,  0x3,    0x2,    0x2,
      0x2,   0x152, 0x150,  0x3,    0x2,    0x2,    0x2,    0x152,  0x153,
      0x3,   0x2,   0x2,    0x2,    0x153,  0x21,   0x3,    0x2,    0x2,
      0x2,   0x154, 0x152,  0x3,    0x2,    0x2,    0x2,    0x155,  0x156,
      0x5,   0x88,  0x45,   0x2,    0x156,  0x157,  0x5,    0x4c,   0x27,
      0x2,   0x157, 0x15a,  0x3,    0x2,    0x2,    0x2,    0x158,  0x15a,
      0x5,   0x90,  0x49,   0x2,    0x159,  0x155,  0x3,    0x2,    0x2,
      0x2,   0x159, 0x158,  0x3,    0x2,    0x2,    0x2,    0x15a,  0x23,
      0x3,   0x2,   0x2,    0x2,    0x15b,  0x160,  0x7,    0x4f,   0x2,
      0x2,   0x15c, 0x15e,  0x7,    0x71,   0x2,    0x2,    0x15d,  0x15c,
      0x3,   0x2,   0x2,    0x2,    0x15d,  0x15e,  0x3,    0x2,    0x2,
      0x2,   0x15e, 0x15f,  0x3,    0x2,    0x2,    0x2,    0x15f,  0x161,
      0x7,   0x50,  0x2,    0x2,    0x160,  0x15d,  0x3,    0x2,    0x2,
      0x2,   0x160, 0x161,  0x3,    0x2,    0x2,    0x2,    0x161,  0x162,
      0x3,   0x2,   0x2,    0x2,    0x162,  0x163,  0x7,    0x71,   0x2,
      0x2,   0x163, 0x168,  0x5,    0x28,   0x15,   0x2,    0x164,  0x166,
      0x7,   0x71,  0x2,    0x2,    0x165,  0x164,  0x3,    0x2,    0x2,
      0x2,   0x165, 0x166,  0x3,    0x2,    0x2,    0x2,    0x166,  0x167,
      0x3,   0x2,   0x2,    0x2,    0x167,  0x169,  0x5,    0x36,   0x1c,
      0x2,   0x168, 0x165,  0x3,    0x2,    0x2,    0x2,    0x168,  0x169,
      0x3,   0x2,   0x2,    0x2,    0x169,  0x25,   0x3,    0x2,    0x2,
      0x2,   0x16a, 0x16f,  0x7,    0x51,   0x2,    0x2,    0x16b,  0x16d,
      0x7,   0x71,  0x2,    0x2,    0x16c,  0x16b,  0x3,    0x2,    0x2,
      0x2,   0x16c, 0x16d,  0x3,    0x2,    0x2,    0x2,    0x16d,  0x16e,
      0x3,   0x2,   0x2,    0x2,    0x16e,  0x170,  0x7,    0x50,   0x2,
      0x2,   0x16f, 0x16c,  0x3,    0x2,    0x2,    0x2,    0x16f,  0x170,
      0x3,   0x2,   0x2,    0x2,    0x170,  0x171,  0x3,    0x2,    0x2,
      0x2,   0x171, 0x172,  0x7,    0x71,   0x2,    0x2,    0x172,  0x173,
      0x5,   0x28,  0x15,   0x2,    0x173,  0x27,   0x3,    0x2,    0x2,
      0x2,   0x174, 0x177,  0x5,    0x2a,   0x16,   0x2,    0x175,  0x176,
      0x7,   0x71,  0x2,    0x2,    0x176,  0x178,  0x5,    0x2e,   0x18,
      0x2,   0x177, 0x175,  0x3,    0x2,    0x2,    0x2,    0x177,  0x178,
      0x3,   0x2,   0x2,    0x2,    0x178,  0x17b,  0x3,    0x2,    0x2,
      0x2,   0x179, 0x17a,  0x7,    0x71,   0x2,    0x2,    0x17a,  0x17c,
      0x5,   0x30,  0x19,   0x2,    0x17b,  0x179,  0x3,    0x2,    0x2,
      0x2,   0x17b, 0x17c,  0x3,    0x2,    0x2,    0x2,    0x17c,  0x17f,
      0x3,   0x2,   0x2,    0x2,    0x17d,  0x17e,  0x7,    0x71,   0x2,
      0x2,   0x17e, 0x180,  0x5,    0x32,   0x1a,   0x2,    0x17f,  0x17d,
      0x3,   0x2,   0x2,    0x2,    0x17f,  0x180,  0x3,    0x2,    0x2,
      0x2,   0x180, 0x29,   0x3,    0x2,    0x2,    0x2,    0x181,  0x18c,
      0x7,   0x7,   0x2,    0x2,    0x182,  0x184,  0x7,    0x71,   0x2,
      0x2,   0x183, 0x182,  0x3,    0x2,    0x2,    0x2,    0x183,  0x184,
      0x3,   0x2,   0x2,    0x2,    0x184,  0x185,  0x3,    0x2,    0x2,
      0x2,   0x185, 0x187,  0x7,    0x4,    0x2,    0x2,    0x186,  0x188,
      0x7,   0x71,  0x2,    0x2,    0x187,  0x186,  0x3,    0x2,    0x2,
      0x2,   0x187, 0x188,  0x3,    0x2,    0x2,    0x2,    0x188,  0x189,
      0x3,   0x2,   0x2,    0x2,    0x189,  0x18b,  0x5,    0x2c,   0x17,
      0x2,   0x18a, 0x183,  0x3,    0x2,    0x2,    0x2,    0x18b,  0x18e,
      0x3,   0x2,   0x2,    0x2,    0x18c,  0x18a,  0x3,    0x2,    0x2,
      0x2,   0x18c, 0x18d,  0x3,    0x2,    0x2,    0x2,    0x18d,  0x19e,
      0x3,   0x2,   0x2,    0x2,    0x18e,  0x18c,  0x3,    0x2,    0x2,
      0x2,   0x18f, 0x19a,  0x5,    0x2c,   0x17,   0x2,    0x190,  0x192,
      0x7,   0x71,  0x2,    0x2,    0x191,  0x190,  0x3,    0x2,    0x2,
      0x2,   0x191, 0x192,  0x3,    0x2,    0x2,    0x2,    0x192,  0x193,
      0x3,   0x2,   0x2,    0x2,    0x193,  0x195,  0x7,    0x4,    0x2,
      0x2,   0x194, 0x196,  0x7,    0x71,   0x2,    0x2,    0x195,  0x194,
      0x3,   0x2,   0x2,    0x2,    0x195,  0x196,  0x3,    0x2,    0x2,
      0x2,   0x196, 0x197,  0x3,    0x2,    0x2,    0x2,    0x197,  0x199,
      0x5,   0x2c,  0x17,   0x2,    0x198,  0x191,  0x3,    0x2,    0x2,
      0x2,   0x199, 0x19c,  0x3,    0x2,    0x2,    0x2,    0x19a,  0x198,
      0x3,   0x2,   0x2,    0x2,    0x19a,  0x19b,  0x3,    0x2,    0x2,
      0x2,   0x19b, 0x19e,  0x3,    0x2,    0x2,    0x2,    0x19c,  0x19a,
      0x3,   0x2,   0x2,    0x2,    0x19d,  0x181,  0x3,    0x2,    0x2,
      0x2,   0x19d, 0x18f,  0x3,    0x2,    0x2,    0x2,    0x19e,  0x2b,
      0x3,   0x2,   0x2,    0x2,    0x19f,  0x1a0,  0x5,    0x56,   0x2c,
      0x2,   0x1a0, 0x1a1,  0x7,    0x71,   0x2,    0x2,    0x1a1,  0x1a2,
      0x7,   0x47,  0x2,    0x2,    0x1a2,  0x1a3,  0x7,    0x71,   0x2,
      0x2,   0x1a3, 0x1a4,  0x5,    0x88,   0x45,   0x2,    0x1a4,  0x1a7,
      0x3,   0x2,   0x2,    0x2,    0x1a5,  0x1a7,  0x5,    0x56,   0x2c,
      0x2,   0x1a6, 0x19f,  0x3,    0x2,    0x2,    0x2,    0x1a6,  0x1a5,
      0x3,   0x2,   0x2,    0x2,    0x1a7,  0x2d,   0x3,    0x2,    0x2,
      0x2,   0x1a8, 0x1a9,  0x7,    0x52,   0x2,    0x2,    0x1a9,  0x1aa,
      0x7,   0x71,  0x2,    0x2,    0x1aa,  0x1ab,  0x7,    0x53,   0x2,
      0x2,   0x1ab, 0x1ac,  0x7,    0x71,   0x2,    0x2,    0x1ac,  0x1b4,
      0x5,   0x34,  0x1b,   0x2,    0x1ad,  0x1af,  0x7,    0x4,    0x2,
      0x2,   0x1ae, 0x1b0,  0x7,    0x71,   0x2,    0x2,    0x1af,  0x1ae,
      0x3,   0x2,   0x2,    0x2,    0x1af,  0x1b0,  0x3,    0x2,    0x2,
      0x2,   0x1b0, 0x1b1,  0x3,    0x2,    0x2,    0x2,    0x1b1,  0x1b3,
      0x5,   0x34,  0x1b,   0x2,    0x1b2,  0x1ad,  0x3,    0x2,    0x2,
      0x2,   0x1b3, 0x1b6,  0x3,    0x2,    0x2,    0x2,    0x1b4,  0x1b2,
      0x3,   0x2,   0x2,    0x2,    0x1b4,  0x1b5,  0x3,    0x2,    0x2,
      0x2,   0x1b5, 0x2f,   0x3,    0x2,    0x2,    0x2,    0x1b6,  0x1b4,
      0x3,   0x2,   0x2,    0x2,    0x1b7,  0x1b8,  0x7,    0x54,   0x2,
      0x2,   0x1b8, 0x1b9,  0x7,    0x71,   0x2,    0x2,    0x1b9,  0x1ba,
      0x5,   0x56,  0x2c,   0x2,    0x1ba,  0x31,   0x3,    0x2,    0x2,
      0x2,   0x1bb, 0x1bc,  0x7,    0x55,   0x2,    0x2,    0x1bc,  0x1bd,
      0x7,   0x71,  0x2,    0x2,    0x1bd,  0x1be,  0x5,    0x56,   0x2c,
      0x2,   0x1be, 0x33,   0x3,    0x2,    0x2,    0x2,    0x1bf,  0x1c4,
      0x5,   0x56,  0x2c,   0x2,    0x1c0,  0x1c2,  0x7,    0x71,   0x2,
      0x2,   0x1c1, 0x1c0,  0x3,    0x2,    0x2,    0x2,    0x1c1,  0x1c2,
      0x3,   0x2,   0x2,    0x2,    0x1c2,  0x1c3,  0x3,    0x2,    0x2,
      0x2,   0x1c3, 0x1c5,  0x9,    0x2,    0x2,    0x2,    0x1c4,  0x1c1,
      0x3,   0x2,   0x2,    0x2,    0x1c4,  0x1c5,  0x3,    0x2,    0x2,
      0x2,   0x1c5, 0x35,   0x3,    0x2,    0x2,    0x2,    0x1c6,  0x1c7,
      0x7,   0x5a,  0x2,    0x2,    0x1c7,  0x1c8,  0x7,    0x71,   0x2,
      0x2,   0x1c8, 0x1c9,  0x5,    0x56,   0x2c,   0x2,    0x1c9,  0x37,
      0x3,   0x2,   0x2,    0x2,    0x1ca,  0x1d5,  0x5,    0x3a,   0x1e,
      0x2,   0x1cb, 0x1cd,  0x7,    0x71,   0x2,    0x2,    0x1cc,  0x1cb,
      0x3,   0x2,   0x2,    0x2,    0x1cc,  0x1cd,  0x3,    0x2,    0x2,
      0x2,   0x1cd, 0x1ce,  0x3,    0x2,    0x2,    0x2,    0x1ce,  0x1d0,
      0x7,   0x4,   0x2,    0x2,    0x1cf,  0x1d1,  0x7,    0x71,   0x2,
      0x2,   0x1d0, 0x1cf,  0x3,    0x2,    0x2,    0x2,    0x1d0,  0x1d1,
      0x3,   0x2,   0x2,    0x2,    0x1d1,  0x1d2,  0x3,    0x2,    0x2,
      0x2,   0x1d2, 0x1d4,  0x5,    0x3a,   0x1e,   0x2,    0x1d3,  0x1cc,
      0x3,   0x2,   0x2,    0x2,    0x1d4,  0x1d7,  0x3,    0x2,    0x2,
      0x2,   0x1d5, 0x1d3,  0x3,    0x2,    0x2,    0x2,    0x1d5,  0x1d6,
      0x3,   0x2,   0x2,    0x2,    0x1d6,  0x39,   0x3,    0x2,    0x2,
      0x2,   0x1d7, 0x1d5,  0x3,    0x2,    0x2,    0x2,    0x1d8,  0x1da,
      0x5,   0x88,  0x45,   0x2,    0x1d9,  0x1db,  0x7,    0x71,   0x2,
      0x2,   0x1da, 0x1d9,  0x3,    0x2,    0x2,    0x2,    0x1da,  0x1db,
      0x3,   0x2,   0x2,    0x2,    0x1db,  0x1dc,  0x3,    0x2,    0x2,
      0x2,   0x1dc, 0x1de,  0x7,    0x5,    0x2,    0x2,    0x1dd,  0x1df,
      0x7,   0x71,  0x2,    0x2,    0x1de,  0x1dd,  0x3,    0x2,    0x2,
      0x2,   0x1de, 0x1df,  0x3,    0x2,    0x2,    0x2,    0x1df,  0x1e0,
      0x3,   0x2,   0x2,    0x2,    0x1e0,  0x1e1,  0x5,    0x3c,   0x1f,
      0x2,   0x1e1, 0x1e4,  0x3,    0x2,    0x2,    0x2,    0x1e2,  0x1e4,
      0x5,   0x3c,  0x1f,   0x2,    0x1e3,  0x1d8,  0x3,    0x2,    0x2,
      0x2,   0x1e3, 0x1e2,  0x3,    0x2,    0x2,    0x2,    0x1e4,  0x3b,
      0x3,   0x2,   0x2,    0x2,    0x1e5,  0x1e6,  0x5,    0x3e,   0x20,
      0x2,   0x1e6, 0x3d,   0x3,    0x2,    0x2,    0x2,    0x1e7,  0x1ee,
      0x5,   0x40,  0x21,   0x2,    0x1e8,  0x1ea,  0x7,    0x71,   0x2,
      0x2,   0x1e9, 0x1e8,  0x3,    0x2,    0x2,    0x2,    0x1e9,  0x1ea,
      0x3,   0x2,   0x2,    0x2,    0x1ea,  0x1eb,  0x3,    0x2,    0x2,
      0x2,   0x1eb, 0x1ed,  0x5,    0x42,   0x22,   0x2,    0x1ec,  0x1e9,
      0x3,   0x2,   0x2,    0x2,    0x1ed,  0x1f0,  0x3,    0x2,    0x2,
      0x2,   0x1ee, 0x1ec,  0x3,    0x2,    0x2,    0x2,    0x1ee,  0x1ef,
      0x3,   0x2,   0x2,    0x2,    0x1ef,  0x1f6,  0x3,    0x2,    0x2,
      0x2,   0x1f0, 0x1ee,  0x3,    0x2,    0x2,    0x2,    0x1f1,  0x1f2,
      0x7,   0x8,   0x2,    0x2,    0x1f2,  0x1f3,  0x5,    0x3e,   0x20,
      0x2,   0x1f3, 0x1f4,  0x7,    0x9,    0x2,    0x2,    0x1f4,  0x1f6,
      0x3,   0x2,   0x2,    0x2,    0x1f5,  0x1e7,  0x3,    0x2,    0x2,
      0x2,   0x1f5, 0x1f1,  0x3,    0x2,    0x2,    0x2,    0x1f6,  0x3f,
      0x3,   0x2,   0x2,    0x2,    0x1f7,  0x1f9,  0x7,    0x8,    0x2,
      0x2,   0x1f8, 0x1fa,  0x7,    0x71,   0x2,    0x2,    0x1f9,  0x1f8,
      0x3,   0x2,   0x2,    0x2,    0x1f9,  0x1fa,  0x3,    0x2,    0x2,
      0x2,   0x1fa, 0x1ff,  0x3,    0x2,    0x2,    0x2,    0x1fb,  0x1fd,
      0x5,   0x88,  0x45,   0x2,    0x1fc,  0x1fe,  0x7,    0x71,   0x2,
      0x2,   0x1fd, 0x1fc,  0x3,    0x2,    0x2,    0x2,    0x1fd,  0x1fe,
      0x3,   0x2,   0x2,    0x2,    0x1fe,  0x200,  0x3,    0x2,    0x2,
      0x2,   0x1ff, 0x1fb,  0x3,    0x2,    0x2,    0x2,    0x1ff,  0x200,
      0x3,   0x2,   0x2,    0x2,    0x200,  0x205,  0x3,    0x2,    0x2,
      0x2,   0x201, 0x203,  0x5,    0x4c,   0x27,   0x2,    0x202,  0x204,
      0x7,   0x71,  0x2,    0x2,    0x203,  0x202,  0x3,    0x2,    0x2,
      0x2,   0x203, 0x204,  0x3,    0x2,    0x2,    0x2,    0x204,  0x206,
      0x3,   0x2,   0x2,    0x2,    0x205,  0x201,  0x3,    0x2,    0x2,
      0x2,   0x205, 0x206,  0x3,    0x2,    0x2,    0x2,    0x206,  0x20b,
      0x3,   0x2,   0x2,    0x2,    0x207,  0x209,  0x5,    0x48,   0x25,
      0x2,   0x208, 0x20a,  0x7,    0x71,   0x2,    0x2,    0x209,  0x208,
      0x3,   0x2,   0x2,    0x2,    0x209,  0x20a,  0x3,    0x2,    0x2,
      0x2,   0x20a, 0x20c,  0x3,    0x2,    0x2,    0x2,    0x20b,  0x207,
      0x3,   0x2,   0x2,    0x2,    0x20b,  0x20c,  0x3,    0x2,    0x2,
      0x2,   0x20c, 0x20d,  0x3,    0x2,    0x2,    0x2,    0x20d,  0x20e,
      0x7,   0x9,   0x2,    0x2,    0x20e,  0x41,   0x3,    0x2,    0x2,
      0x2,   0x20f, 0x211,  0x5,    0x44,   0x23,   0x2,    0x210,  0x212,
      0x7,   0x71,  0x2,    0x2,    0x211,  0x210,  0x3,    0x2,    0x2,
      0x2,   0x211, 0x212,  0x3,    0x2,    0x2,    0x2,    0x212,  0x213,
      0x3,   0x2,   0x2,    0x2,    0x213,  0x214,  0x5,    0x40,   0x21,
      0x2,   0x214, 0x43,   0x3,    0x2,    0x2,    0x2,    0x215,  0x217,
      0x5,   0x9a,  0x4e,   0x2,    0x216,  0x218,  0x7,    0x71,   0x2,
      0x2,   0x217, 0x216,  0x3,    0x2,    0x2,    0x2,    0x217,  0x218,
      0x3,   0x2,   0x2,    0x2,    0x218,  0x219,  0x3,    0x2,    0x2,
      0x2,   0x219, 0x21b,  0x5,    0x9e,   0x50,   0x2,    0x21a,  0x21c,
      0x7,   0x71,  0x2,    0x2,    0x21b,  0x21a,  0x3,    0x2,    0x2,
      0x2,   0x21b, 0x21c,  0x3,    0x2,    0x2,    0x2,    0x21c,  0x21e,
      0x3,   0x2,   0x2,    0x2,    0x21d,  0x21f,  0x5,    0x46,   0x24,
      0x2,   0x21e, 0x21d,  0x3,    0x2,    0x2,    0x2,    0x21e,  0x21f,
      0x3,   0x2,   0x2,    0x2,    0x21f,  0x221,  0x3,    0x2,    0x2,
      0x2,   0x220, 0x222,  0x7,    0x71,   0x2,    0x2,    0x221,  0x220,
      0x3,   0x2,   0x2,    0x2,    0x221,  0x222,  0x3,    0x2,    0x2,
      0x2,   0x222, 0x223,  0x3,    0x2,    0x2,    0x2,    0x223,  0x225,
      0x5,   0x9e,  0x50,   0x2,    0x224,  0x226,  0x7,    0x71,   0x2,
      0x2,   0x225, 0x224,  0x3,    0x2,    0x2,    0x2,    0x225,  0x226,
      0x3,   0x2,   0x2,    0x2,    0x226,  0x227,  0x3,    0x2,    0x2,
      0x2,   0x227, 0x228,  0x5,    0x9c,   0x4f,   0x2,    0x228,  0x256,
      0x3,   0x2,   0x2,    0x2,    0x229,  0x22b,  0x5,    0x9a,   0x4e,
      0x2,   0x22a, 0x22c,  0x7,    0x71,   0x2,    0x2,    0x22b,  0x22a,
      0x3,   0x2,   0x2,    0x2,    0x22b,  0x22c,  0x3,    0x2,    0x2,
      0x2,   0x22c, 0x22d,  0x3,    0x2,    0x2,    0x2,    0x22d,  0x22f,
      0x5,   0x9e,  0x50,   0x2,    0x22e,  0x230,  0x7,    0x71,   0x2,
      0x2,   0x22f, 0x22e,  0x3,    0x2,    0x2,    0x2,    0x22f,  0x230,
      0x3,   0x2,   0x2,    0x2,    0x230,  0x232,  0x3,    0x2,    0x2,
      0x2,   0x231, 0x233,  0x5,    0x46,   0x24,   0x2,    0x232,  0x231,
      0x3,   0x2,   0x2,    0x2,    0x232,  0x233,  0x3,    0x2,    0x2,
      0x2,   0x233, 0x235,  0x3,    0x2,    0x2,    0x2,    0x234,  0x236,
      0x7,   0x71,  0x2,    0x2,    0x235,  0x234,  0x3,    0x2,    0x2,
      0x2,   0x235, 0x236,  0x3,    0x2,    0x2,    0x2,    0x236,  0x237,
      0x3,   0x2,   0x2,    0x2,    0x237,  0x238,  0x5,    0x9e,   0x50,
      0x2,   0x238, 0x256,  0x3,    0x2,    0x2,    0x2,    0x239,  0x23b,
      0x5,   0x9e,  0x50,   0x2,    0x23a,  0x23c,  0x7,    0x71,   0x2,
      0x2,   0x23b, 0x23a,  0x3,    0x2,    0x2,    0x2,    0x23b,  0x23c,
      0x3,   0x2,   0x2,    0x2,    0x23c,  0x23e,  0x3,    0x2,    0x2,
      0x2,   0x23d, 0x23f,  0x5,    0x46,   0x24,   0x2,    0x23e,  0x23d,
      0x3,   0x2,   0x2,    0x2,    0x23e,  0x23f,  0x3,    0x2,    0x2,
      0x2,   0x23f, 0x241,  0x3,    0x2,    0x2,    0x2,    0x240,  0x242,
      0x7,   0x71,  0x2,    0x2,    0x241,  0x240,  0x3,    0x2,    0x2,
      0x2,   0x241, 0x242,  0x3,    0x2,    0x2,    0x2,    0x242,  0x243,
      0x3,   0x2,   0x2,    0x2,    0x243,  0x245,  0x5,    0x9e,   0x50,
      0x2,   0x244, 0x246,  0x7,    0x71,   0x2,    0x2,    0x245,  0x244,
      0x3,   0x2,   0x2,    0x2,    0x245,  0x246,  0x3,    0x2,    0x2,
      0x2,   0x246, 0x247,  0x3,    0x2,    0x2,    0x2,    0x247,  0x248,
      0x5,   0x9c,  0x4f,   0x2,    0x248,  0x256,  0x3,    0x2,    0x2,
      0x2,   0x249, 0x24b,  0x5,    0x9e,   0x50,   0x2,    0x24a,  0x24c,
      0x7,   0x71,  0x2,    0x2,    0x24b,  0x24a,  0x3,    0x2,    0x2,
      0x2,   0x24b, 0x24c,  0x3,    0x2,    0x2,    0x2,    0x24c,  0x24e,
      0x3,   0x2,   0x2,    0x2,    0x24d,  0x24f,  0x5,    0x46,   0x24,
      0x2,   0x24e, 0x24d,  0x3,    0x2,    0x2,    0x2,    0x24e,  0x24f,
      0x3,   0x2,   0x2,    0x2,    0x24f,  0x251,  0x3,    0x2,    0x2,
      0x2,   0x250, 0x252,  0x7,    0x71,   0x2,    0x2,    0x251,  0x250,
      0x3,   0x2,   0x2,    0x2,    0x251,  0x252,  0x3,    0x2,    0x2,
      0x2,   0x252, 0x253,  0x3,    0x2,    0x2,    0x2,    0x253,  0x254,
      0x5,   0x9e,  0x50,   0x2,    0x254,  0x256,  0x3,    0x2,    0x2,
      0x2,   0x255, 0x215,  0x3,    0x2,    0x2,    0x2,    0x255,  0x229,
      0x3,   0x2,   0x2,    0x2,    0x255,  0x239,  0x3,    0x2,    0x2,
      0x2,   0x255, 0x249,  0x3,    0x2,    0x2,    0x2,    0x256,  0x45,
      0x3,   0x2,   0x2,    0x2,    0x257,  0x259,  0x7,    0xa,    0x2,
      0x2,   0x258, 0x25a,  0x5,    0x88,   0x45,   0x2,    0x259,  0x258,
      0x3,   0x2,   0x2,    0x2,    0x259,  0x25a,  0x3,    0x2,    0x2,
      0x2,   0x25a, 0x25c,  0x3,    0x2,    0x2,    0x2,    0x25b,  0x25d,
      0x7,   0xb,   0x2,    0x2,    0x25c,  0x25b,  0x3,    0x2,    0x2,
      0x2,   0x25c, 0x25d,  0x3,    0x2,    0x2,    0x2,    0x25d,  0x25f,
      0x3,   0x2,   0x2,    0x2,    0x25e,  0x260,  0x5,    0x4a,   0x26,
      0x2,   0x25f, 0x25e,  0x3,    0x2,    0x2,    0x2,    0x25f,  0x260,
      0x3,   0x2,   0x2,    0x2,    0x260,  0x262,  0x3,    0x2,    0x2,
      0x2,   0x261, 0x263,  0x5,    0x50,   0x29,   0x2,    0x262,  0x261,
      0x3,   0x2,   0x2,    0x2,    0x262,  0x263,  0x3,    0x2,    0x2,
      0x2,   0x263, 0x265,  0x3,    0x2,    0x2,    0x2,    0x264,  0x266,
      0x5,   0x48,  0x25,   0x2,    0x265,  0x264,  0x3,    0x2,    0x2,
      0x2,   0x265, 0x266,  0x3,    0x2,    0x2,    0x2,    0x266,  0x267,
      0x3,   0x2,   0x2,    0x2,    0x267,  0x268,  0x7,    0xc,    0x2,
      0x2,   0x268, 0x47,   0x3,    0x2,    0x2,    0x2,    0x269,  0x26c,
      0x5,   0x8c,  0x47,   0x2,    0x26a,  0x26c,  0x5,    0x8e,   0x48,
      0x2,   0x26b, 0x269,  0x3,    0x2,    0x2,    0x2,    0x26b,  0x26a,
      0x3,   0x2,   0x2,    0x2,    0x26c,  0x49,   0x3,    0x2,    0x2,
      0x2,   0x26d, 0x26e,  0x7,    0xd,    0x2,    0x2,    0x26e,  0x27c,
      0x5,   0x54,  0x2b,   0x2,    0x26f,  0x271,  0x7,    0x71,   0x2,
      0x2,   0x270, 0x26f,  0x3,    0x2,    0x2,    0x2,    0x270,  0x271,
      0x3,   0x2,   0x2,    0x2,    0x271,  0x272,  0x3,    0x2,    0x2,
      0x2,   0x272, 0x274,  0x7,    0xe,    0x2,    0x2,    0x273,  0x275,
      0x7,   0xd,   0x2,    0x2,    0x274,  0x273,  0x3,    0x2,    0x2,
      0x2,   0x274, 0x275,  0x3,    0x2,    0x2,    0x2,    0x275,  0x277,
      0x3,   0x2,   0x2,    0x2,    0x276,  0x278,  0x7,    0x71,   0x2,
      0x2,   0x277, 0x276,  0x3,    0x2,    0x2,    0x2,    0x277,  0x278,
      0x3,   0x2,   0x2,    0x2,    0x278,  0x279,  0x3,    0x2,    0x2,
      0x2,   0x279, 0x27b,  0x5,    0x54,   0x2b,   0x2,    0x27a,  0x270,
      0x3,   0x2,   0x2,    0x2,    0x27b,  0x27e,  0x3,    0x2,    0x2,
      0x2,   0x27c, 0x27a,  0x3,    0x2,    0x2,    0x2,    0x27c,  0x27d,
      0x3,   0x2,   0x2,    0x2,    0x27d,  0x4b,   0x3,    0x2,    0x2,
      0x2,   0x27e, 0x27c,  0x3,    0x2,    0x2,    0x2,    0x27f,  0x286,
      0x5,   0x4e,  0x28,   0x2,    0x280,  0x282,  0x7,    0x71,   0x2,
      0x2,   0x281, 0x280,  0x3,    0x2,    0x2,    0x2,    0x281,  0x282,
      0x3,   0x2,   0x2,    0x2,    0x282,  0x283,  0x3,    0x2,    0x2,
      0x2,   0x283, 0x285,  0x5,    0x4e,   0x28,   0x2,    0x284,  0x281,
      0x3,   0x2,   0x2,    0x2,    0x285,  0x288,  0x3,    0x2,    0x2,
      0x2,   0x286, 0x284,  0x3,    0x2,    0x2,    0x2,    0x286,  0x287,
      0x3,   0x2,   0x2,    0x2,    0x287,  0x4d,   0x3,    0x2,    0x2,
      0x2,   0x288, 0x286,  0x3,    0x2,    0x2,    0x2,    0x289,  0x28a,
      0x7,   0xd,   0x2,    0x2,    0x28a,  0x28b,  0x5,    0x52,   0x2a,
      0x2,   0x28b, 0x4f,   0x3,    0x2,    0x2,    0x2,    0x28c,  0x28e,
      0x7,   0x7,   0x2,    0x2,    0x28d,  0x28f,  0x7,    0x71,   0x2,
      0x2,   0x28e, 0x28d,  0x3,    0x2,    0x2,    0x2,    0x28e,  0x28f,
      0x3,   0x2,   0x2,    0x2,    0x28f,  0x294,  0x3,    0x2,    0x2,
      0x2,   0x290, 0x292,  0x5,    0x94,   0x4b,   0x2,    0x291,  0x293,
      0x7,   0x71,  0x2,    0x2,    0x292,  0x291,  0x3,    0x2,    0x2,
      0x2,   0x292, 0x293,  0x3,    0x2,    0x2,    0x2,    0x293,  0x295,
      0x3,   0x2,   0x2,    0x2,    0x294,  0x290,  0x3,    0x2,    0x2,
      0x2,   0x294, 0x295,  0x3,    0x2,    0x2,    0x2,    0x295,  0x2a0,
      0x3,   0x2,   0x2,    0x2,    0x296,  0x298,  0x7,    0xf,    0x2,
      0x2,   0x297, 0x299,  0x7,    0x71,   0x2,    0x2,    0x298,  0x297,
      0x3,   0x2,   0x2,    0x2,    0x298,  0x299,  0x3,    0x2,    0x2,
      0x2,   0x299, 0x29e,  0x3,    0x2,    0x2,    0x2,    0x29a,  0x29c,
      0x5,   0x94,  0x4b,   0x2,    0x29b,  0x29d,  0x7,    0x71,   0x2,
      0x2,   0x29c, 0x29b,  0x3,    0x2,    0x2,    0x2,    0x29c,  0x29d,
      0x3,   0x2,   0x2,    0x2,    0x29d,  0x29f,  0x3,    0x2,    0x2,
      0x2,   0x29e, 0x29a,  0x3,    0x2,    0x2,    0x2,    0x29e,  0x29f,
      0x3,   0x2,   0x2,    0x2,    0x29f,  0x2a1,  0x3,    0x2,    0x2,
      0x2,   0x2a0, 0x296,  0x3,    0x2,    0x2,    0x2,    0x2a0,  0x2a1,
      0x3,   0x2,   0x2,    0x2,    0x2a1,  0x51,   0x3,    0x2,    0x2,
      0x2,   0x2a2, 0x2a3,  0x5,    0x98,   0x4d,   0x2,    0x2a3,  0x53,
      0x3,   0x2,   0x2,    0x2,    0x2a4,  0x2a5,  0x5,    0x98,   0x4d,
      0x2,   0x2a5, 0x55,   0x3,    0x2,    0x2,    0x2,    0x2a6,  0x2a7,
      0x5,   0x58,  0x2d,   0x2,    0x2a7,  0x57,   0x3,    0x2,    0x2,
      0x2,   0x2a8, 0x2af,  0x5,    0x5a,   0x2e,   0x2,    0x2a9,  0x2aa,
      0x7,   0x71,  0x2,    0x2,    0x2aa,  0x2ab,  0x7,    0x5b,   0x2,
      0x2,   0x2ab, 0x2ac,  0x7,    0x71,   0x2,    0x2,    0x2ac,  0x2ae,
      0x5,   0x5a,  0x2e,   0x2,    0x2ad,  0x2a9,  0x3,    0x2,    0x2,
      0x2,   0x2ae, 0x2b1,  0x3,    0x2,    0x2,    0x2,    0x2af,  0x2ad,
      0x3,   0x2,   0x2,    0x2,    0x2af,  0x2b0,  0x3,    0x2,    0x2,
      0x2,   0x2b0, 0x59,   0x3,    0x2,    0x2,    0x2,    0x2b1,  0x2af,
      0x3,   0x2,   0x2,    0x2,    0x2b2,  0x2b9,  0x5,    0x5c,   0x2f,
      0x2,   0x2b3, 0x2b4,  0x7,    0x71,   0x2,    0x2,    0x2b4,  0x2b5,
      0x7,   0x5c,  0x2,    0x2,    0x2b5,  0x2b6,  0x7,    0x71,   0x2,
      0x2,   0x2b6, 0x2b8,  0x5,    0x5c,   0x2f,   0x2,    0x2b7,  0x2b3,
      0x3,   0x2,   0x2,    0x2,    0x2b8,  0x2bb,  0x3,    0x2,    0x2,
      0x2,   0x2b9, 0x2b7,  0x3,    0x2,    0x2,    0x2,    0x2b9,  0x2ba,
      0x3,   0x2,   0x2,    0x2,    0x2ba,  0x5b,   0x3,    0x2,    0x2,
      0x2,   0x2bb, 0x2b9,  0x3,    0x2,    0x2,    0x2,    0x2bc,  0x2c3,
      0x5,   0x5e,  0x30,   0x2,    0x2bd,  0x2be,  0x7,    0x71,   0x2,
      0x2,   0x2be, 0x2bf,  0x7,    0x5d,   0x2,    0x2,    0x2bf,  0x2c0,
      0x7,   0x71,  0x2,    0x2,    0x2c0,  0x2c2,  0x5,    0x5e,   0x30,
      0x2,   0x2c1, 0x2bd,  0x3,    0x2,    0x2,    0x2,    0x2c2,  0x2c5,
      0x3,   0x2,   0x2,    0x2,    0x2c3,  0x2c1,  0x3,    0x2,    0x2,
      0x2,   0x2c3, 0x2c4,  0x3,    0x2,    0x2,    0x2,    0x2c4,  0x5d,
      0x3,   0x2,   0x2,    0x2,    0x2c5,  0x2c3,  0x3,    0x2,    0x2,
      0x2,   0x2c6, 0x2c8,  0x7,    0x5e,   0x2,    0x2,    0x2c7,  0x2c9,
      0x7,   0x71,  0x2,    0x2,    0x2c8,  0x2c7,  0x3,    0x2,    0x2,
      0x2,   0x2c8, 0x2c9,  0x3,    0x2,    0x2,    0x2,    0x2c9,  0x2cb,
      0x3,   0x2,   0x2,    0x2,    0x2ca,  0x2c6,  0x3,    0x2,    0x2,
      0x2,   0x2cb, 0x2ce,  0x3,    0x2,    0x2,    0x2,    0x2cc,  0x2ca,
      0x3,   0x2,   0x2,    0x2,    0x2cc,  0x2cd,  0x3,    0x2,    0x2,
      0x2,   0x2cd, 0x2cf,  0x3,    0x2,    0x2,    0x2,    0x2ce,  0x2cc,
      0x3,   0x2,   0x2,    0x2,    0x2cf,  0x2d0,  0x5,    0x60,   0x31,
      0x2,   0x2d0, 0x5f,   0x3,    0x2,    0x2,    0x2,    0x2d1,  0x2d8,
      0x5,   0x62,  0x32,   0x2,    0x2d2,  0x2d4,  0x7,    0x71,   0x2,
      0x2,   0x2d3, 0x2d2,  0x3,    0x2,    0x2,    0x2,    0x2d3,  0x2d4,
      0x3,   0x2,   0x2,    0x2,    0x2d4,  0x2d5,  0x3,    0x2,    0x2,
      0x2,   0x2d5, 0x2d7,  0x5,    0x76,   0x3c,   0x2,    0x2d6,  0x2d3,
      0x3,   0x2,   0x2,    0x2,    0x2d7,  0x2da,  0x3,    0x2,    0x2,
      0x2,   0x2d8, 0x2d6,  0x3,    0x2,    0x2,    0x2,    0x2d8,  0x2d9,
      0x3,   0x2,   0x2,    0x2,    0x2d9,  0x61,   0x3,    0x2,    0x2,
      0x2,   0x2da, 0x2d8,  0x3,    0x2,    0x2,    0x2,    0x2db,  0x2ee,
      0x5,   0x64,  0x33,   0x2,    0x2dc,  0x2de,  0x7,    0x71,   0x2,
      0x2,   0x2dd, 0x2dc,  0x3,    0x2,    0x2,    0x2,    0x2dd,  0x2de,
      0x3,   0x2,   0x2,    0x2,    0x2de,  0x2df,  0x3,    0x2,    0x2,
      0x2,   0x2df, 0x2e1,  0x7,    0x10,   0x2,    0x2,    0x2e0,  0x2e2,
      0x7,   0x71,  0x2,    0x2,    0x2e1,  0x2e0,  0x3,    0x2,    0x2,
      0x2,   0x2e1, 0x2e2,  0x3,    0x2,    0x2,    0x2,    0x2e2,  0x2e3,
      0x3,   0x2,   0x2,    0x2,    0x2e3,  0x2ed,  0x5,    0x64,   0x33,
      0x2,   0x2e4, 0x2e6,  0x7,    0x71,   0x2,    0x2,    0x2e5,  0x2e4,
      0x3,   0x2,   0x2,    0x2,    0x2e5,  0x2e6,  0x3,    0x2,    0x2,
      0x2,   0x2e6, 0x2e7,  0x3,    0x2,    0x2,    0x2,    0x2e7,  0x2e9,
      0x7,   0x11,  0x2,    0x2,    0x2e8,  0x2ea,  0x7,    0x71,   0x2,
      0x2,   0x2e9, 0x2e8,  0x3,    0x2,    0x2,    0x2,    0x2e9,  0x2ea,
      0x3,   0x2,   0x2,    0x2,    0x2ea,  0x2eb,  0x3,    0x2,    0x2,
      0x2,   0x2eb, 0x2ed,  0x5,    0x64,   0x33,   0x2,    0x2ec,  0x2dd,
      0x3,   0x2,   0x2,    0x2,    0x2ec,  0x2e5,  0x3,    0x2,    0x2,
      0x2,   0x2ed, 0x2f0,  0x3,    0x2,    0x2,    0x2,    0x2ee,  0x2ec,
      0x3,   0x2,   0x2,    0x2,    0x2ee,  0x2ef,  0x3,    0x2,    0x2,
      0x2,   0x2ef, 0x63,   0x3,    0x2,    0x2,    0x2,    0x2f0,  0x2ee,
      0x3,   0x2,   0x2,    0x2,    0x2f1,  0x30c,  0x5,    0x66,   0x34,
      0x2,   0x2f2, 0x2f4,  0x7,    0x71,   0x2,    0x2,    0x2f3,  0x2f2,
      0x3,   0x2,   0x2,    0x2,    0x2f3,  0x2f4,  0x3,    0x2,    0x2,
      0x2,   0x2f4, 0x2f5,  0x3,    0x2,    0x2,    0x2,    0x2f5,  0x2f7,
      0x7,   0x7,   0x2,    0x2,    0x2f6,  0x2f8,  0x7,    0x71,   0x2,
      0x2,   0x2f7, 0x2f6,  0x3,    0x2,    0x2,    0x2,    0x2f7,  0x2f8,
      0x3,   0x2,   0x2,    0x2,    0x2f8,  0x2f9,  0x3,    0x2,    0x2,
      0x2,   0x2f9, 0x30b,  0x5,    0x66,   0x34,   0x2,    0x2fa,  0x2fc,
      0x7,   0x71,  0x2,    0x2,    0x2fb,  0x2fa,  0x3,    0x2,    0x2,
      0x2,   0x2fb, 0x2fc,  0x3,    0x2,    0x2,    0x2,    0x2fc,  0x2fd,
      0x3,   0x2,   0x2,    0x2,    0x2fd,  0x2ff,  0x7,    0x12,   0x2,
      0x2,   0x2fe, 0x300,  0x7,    0x71,   0x2,    0x2,    0x2ff,  0x2fe,
      0x3,   0x2,   0x2,    0x2,    0x2ff,  0x300,  0x3,    0x2,    0x2,
      0x2,   0x300, 0x301,  0x3,    0x2,    0x2,    0x2,    0x301,  0x30b,
      0x5,   0x66,  0x34,   0x2,    0x302,  0x304,  0x7,    0x71,   0x2,
      0x2,   0x303, 0x302,  0x3,    0x2,    0x2,    0x2,    0x303,  0x304,
      0x3,   0x2,   0x2,    0x2,    0x304,  0x305,  0x3,    0x2,    0x2,
      0x2,   0x305, 0x307,  0x7,    0x13,   0x2,    0x2,    0x306,  0x308,
      0x7,   0x71,  0x2,    0x2,    0x307,  0x306,  0x3,    0x2,    0x2,
      0x2,   0x307, 0x308,  0x3,    0x2,    0x2,    0x2,    0x308,  0x309,
      0x3,   0x2,   0x2,    0x2,    0x309,  0x30b,  0x5,    0x66,   0x34,
      0x2,   0x30a, 0x2f3,  0x3,    0x2,    0x2,    0x2,    0x30a,  0x2fb,
      0x3,   0x2,   0x2,    0x2,    0x30a,  0x303,  0x3,    0x2,    0x2,
      0x2,   0x30b, 0x30e,  0x3,    0x2,    0x2,    0x2,    0x30c,  0x30a,
      0x3,   0x2,   0x2,    0x2,    0x30c,  0x30d,  0x3,    0x2,    0x2,
      0x2,   0x30d, 0x65,   0x3,    0x2,    0x2,    0x2,    0x30e,  0x30c,
      0x3,   0x2,   0x2,    0x2,    0x30f,  0x31a,  0x5,    0x68,   0x35,
      0x2,   0x310, 0x312,  0x7,    0x71,   0x2,    0x2,    0x311,  0x310,
      0x3,   0x2,   0x2,    0x2,    0x311,  0x312,  0x3,    0x2,    0x2,
      0x2,   0x312, 0x313,  0x3,    0x2,    0x2,    0x2,    0x313,  0x315,
      0x7,   0x14,  0x2,    0x2,    0x314,  0x316,  0x7,    0x71,   0x2,
      0x2,   0x315, 0x314,  0x3,    0x2,    0x2,    0x2,    0x315,  0x316,
      0x3,   0x2,   0x2,    0x2,    0x316,  0x317,  0x3,    0x2,    0x2,
      0x2,   0x317, 0x319,  0x5,    0x68,   0x35,   0x2,    0x318,  0x311,
      0x3,   0x2,   0x2,    0x2,    0x319,  0x31c,  0x3,    0x2,    0x2,
      0x2,   0x31a, 0x318,  0x3,    0x2,    0x2,    0x2,    0x31a,  0x31b,
      0x3,   0x2,   0x2,    0x2,    0x31b,  0x67,   0x3,    0x2,    0x2,
      0x2,   0x31c, 0x31a,  0x3,    0x2,    0x2,    0x2,    0x31d,  0x31f,
      0x9,   0x3,   0x2,    0x2,    0x31e,  0x320,  0x7,    0x71,   0x2,
      0x2,   0x31f, 0x31e,  0x3,    0x2,    0x2,    0x2,    0x31f,  0x320,
      0x3,   0x2,   0x2,    0x2,    0x320,  0x322,  0x3,    0x2,    0x2,
      0x2,   0x321, 0x31d,  0x3,    0x2,    0x2,    0x2,    0x322,  0x325,
      0x3,   0x2,   0x2,    0x2,    0x323,  0x321,  0x3,    0x2,    0x2,
      0x2,   0x323, 0x324,  0x3,    0x2,    0x2,    0x2,    0x324,  0x326,
      0x3,   0x2,   0x2,    0x2,    0x325,  0x323,  0x3,    0x2,    0x2,
      0x2,   0x326, 0x327,  0x5,    0x6a,   0x36,   0x2,    0x327,  0x69,
      0x3,   0x2,   0x2,    0x2,    0x328,  0x35e,  0x5,    0x6c,   0x37,
      0x2,   0x329, 0x32b,  0x7,    0x71,   0x2,    0x2,    0x32a,  0x329,
      0x3,   0x2,   0x2,    0x2,    0x32a,  0x32b,  0x3,    0x2,    0x2,
      0x2,   0x32b, 0x32c,  0x3,    0x2,    0x2,    0x2,    0x32c,  0x32d,
      0x7,   0xa,   0x2,    0x2,    0x32d,  0x32e,  0x5,    0x56,   0x2c,
      0x2,   0x32e, 0x32f,  0x7,    0xc,    0x2,    0x2,    0x32f,  0x35d,
      0x3,   0x2,   0x2,    0x2,    0x330,  0x332,  0x7,    0x71,   0x2,
      0x2,   0x331, 0x330,  0x3,    0x2,    0x2,    0x2,    0x331,  0x332,
      0x3,   0x2,   0x2,    0x2,    0x332,  0x333,  0x3,    0x2,    0x2,
      0x2,   0x333, 0x335,  0x7,    0xa,    0x2,    0x2,    0x334,  0x336,
      0x5,   0x56,  0x2c,   0x2,    0x335,  0x334,  0x3,    0x2,    0x2,
      0x2,   0x335, 0x336,  0x3,    0x2,    0x2,    0x2,    0x336,  0x337,
      0x3,   0x2,   0x2,    0x2,    0x337,  0x339,  0x7,    0xf,    0x2,
      0x2,   0x338, 0x33a,  0x5,    0x56,   0x2c,   0x2,    0x339,  0x338,
      0x3,   0x2,   0x2,    0x2,    0x339,  0x33a,  0x3,    0x2,    0x2,
      0x2,   0x33a, 0x33b,  0x3,    0x2,    0x2,    0x2,    0x33b,  0x35d,
      0x7,   0xc,   0x2,    0x2,    0x33c,  0x33e,  0x7,    0x71,   0x2,
      0x2,   0x33d, 0x33c,  0x3,    0x2,    0x2,    0x2,    0x33d,  0x33e,
      0x3,   0x2,   0x2,    0x2,    0x33e,  0x33f,  0x3,    0x2,    0x2,
      0x2,   0x33f, 0x34d,  0x7,    0x15,   0x2,    0x2,    0x340,  0x341,
      0x7,   0x71,  0x2,    0x2,    0x341,  0x34d,  0x7,    0x5f,   0x2,
      0x2,   0x342, 0x343,  0x7,    0x71,   0x2,    0x2,    0x343,  0x344,
      0x7,   0x60,  0x2,    0x2,    0x344,  0x345,  0x7,    0x71,   0x2,
      0x2,   0x345, 0x34d,  0x7,    0x4f,   0x2,    0x2,    0x346,  0x347,
      0x7,   0x71,  0x2,    0x2,    0x347,  0x348,  0x7,    0x61,   0x2,
      0x2,   0x348, 0x349,  0x7,    0x71,   0x2,    0x2,    0x349,  0x34d,
      0x7,   0x4f,  0x2,    0x2,    0x34a,  0x34b,  0x7,    0x71,   0x2,
      0x2,   0x34b, 0x34d,  0x7,    0x62,   0x2,    0x2,    0x34c,  0x33d,
      0x3,   0x2,   0x2,    0x2,    0x34c,  0x340,  0x3,    0x2,    0x2,
      0x2,   0x34c, 0x342,  0x3,    0x2,    0x2,    0x2,    0x34c,  0x346,
      0x3,   0x2,   0x2,    0x2,    0x34c,  0x34a,  0x3,    0x2,    0x2,
      0x2,   0x34d, 0x34f,  0x3,    0x2,    0x2,    0x2,    0x34e,  0x350,
      0x7,   0x71,  0x2,    0x2,    0x34f,  0x34e,  0x3,    0x2,    0x2,
      0x2,   0x34f, 0x350,  0x3,    0x2,    0x2,    0x2,    0x350,  0x351,
      0x3,   0x2,   0x2,    0x2,    0x351,  0x35d,  0x5,    0x6c,   0x37,
      0x2,   0x352, 0x353,  0x7,    0x71,   0x2,    0x2,    0x353,  0x354,
      0x7,   0x63,  0x2,    0x2,    0x354,  0x355,  0x7,    0x71,   0x2,
      0x2,   0x355, 0x35d,  0x7,    0x64,   0x2,    0x2,    0x356,  0x357,
      0x7,   0x71,  0x2,    0x2,    0x357,  0x358,  0x7,    0x63,   0x2,
      0x2,   0x358, 0x359,  0x7,    0x71,   0x2,    0x2,    0x359,  0x35a,
      0x7,   0x5e,  0x2,    0x2,    0x35a,  0x35b,  0x7,    0x71,   0x2,
      0x2,   0x35b, 0x35d,  0x7,    0x64,   0x2,    0x2,    0x35c,  0x32a,
      0x3,   0x2,   0x2,    0x2,    0x35c,  0x331,  0x3,    0x2,    0x2,
      0x2,   0x35c, 0x34c,  0x3,    0x2,    0x2,    0x2,    0x35c,  0x352,
      0x3,   0x2,   0x2,    0x2,    0x35c,  0x356,  0x3,    0x2,    0x2,
      0x2,   0x35d, 0x360,  0x3,    0x2,    0x2,    0x2,    0x35e,  0x35c,
      0x3,   0x2,   0x2,    0x2,    0x35e,  0x35f,  0x3,    0x2,    0x2,
      0x2,   0x35f, 0x6b,   0x3,    0x2,    0x2,    0x2,    0x360,  0x35e,
      0x3,   0x2,   0x2,    0x2,    0x361,  0x366,  0x5,    0x6e,   0x38,
      0x2,   0x362, 0x365,  0x5,    0x86,   0x44,   0x2,    0x363,  0x365,
      0x5,   0x4c,  0x27,   0x2,    0x364,  0x362,  0x3,    0x2,    0x2,
      0x2,   0x364, 0x363,  0x3,    0x2,    0x2,    0x2,    0x365,  0x368,
      0x3,   0x2,   0x2,    0x2,    0x366,  0x364,  0x3,    0x2,    0x2,
      0x2,   0x366, 0x367,  0x3,    0x2,    0x2,    0x2,    0x367,  0x6d,
      0x3,   0x2,   0x2,    0x2,    0x368,  0x366,  0x3,    0x2,    0x2,
      0x2,   0x369, 0x3d9,  0x5,    0x70,   0x39,   0x2,    0x36a,  0x3d9,
      0x5,   0x8e,  0x48,   0x2,    0x36b,  0x36d,  0x7,    0x65,   0x2,
      0x2,   0x36c, 0x36e,  0x7,    0x71,   0x2,    0x2,    0x36d,  0x36c,
      0x3,   0x2,   0x2,    0x2,    0x36d,  0x36e,  0x3,    0x2,    0x2,
      0x2,   0x36e, 0x36f,  0x3,    0x2,    0x2,    0x2,    0x36f,  0x371,
      0x7,   0x8,   0x2,    0x2,    0x370,  0x372,  0x7,    0x71,   0x2,
      0x2,   0x371, 0x370,  0x3,    0x2,    0x2,    0x2,    0x371,  0x372,
      0x3,   0x2,   0x2,    0x2,    0x372,  0x373,  0x3,    0x2,    0x2,
      0x2,   0x373, 0x375,  0x7,    0x7,    0x2,    0x2,    0x374,  0x376,
      0x7,   0x71,  0x2,    0x2,    0x375,  0x374,  0x3,    0x2,    0x2,
      0x2,   0x375, 0x376,  0x3,    0x2,    0x2,    0x2,    0x376,  0x377,
      0x3,   0x2,   0x2,    0x2,    0x377,  0x3d9,  0x7,    0x9,    0x2,
      0x2,   0x378, 0x3d9,  0x5,    0x84,   0x43,   0x2,    0x379,  0x37b,
      0x7,   0x66,  0x2,    0x2,    0x37a,  0x37c,  0x7,    0x71,   0x2,
      0x2,   0x37b, 0x37a,  0x3,    0x2,    0x2,    0x2,    0x37b,  0x37c,
      0x3,   0x2,   0x2,    0x2,    0x37c,  0x37d,  0x3,    0x2,    0x2,
      0x2,   0x37d, 0x37f,  0x7,    0x8,    0x2,    0x2,    0x37e,  0x380,
      0x7,   0x71,  0x2,    0x2,    0x37f,  0x37e,  0x3,    0x2,    0x2,
      0x2,   0x37f, 0x380,  0x3,    0x2,    0x2,    0x2,    0x380,  0x381,
      0x3,   0x2,   0x2,    0x2,    0x381,  0x383,  0x5,    0x7c,   0x3f,
      0x2,   0x382, 0x384,  0x7,    0x71,   0x2,    0x2,    0x383,  0x382,
      0x3,   0x2,   0x2,    0x2,    0x383,  0x384,  0x3,    0x2,    0x2,
      0x2,   0x384, 0x385,  0x3,    0x2,    0x2,    0x2,    0x385,  0x386,
      0x7,   0x9,   0x2,    0x2,    0x386,  0x3d9,  0x3,    0x2,    0x2,
      0x2,   0x387, 0x389,  0x7,    0x67,   0x2,    0x2,    0x388,  0x38a,
      0x7,   0x71,  0x2,    0x2,    0x389,  0x388,  0x3,    0x2,    0x2,
      0x2,   0x389, 0x38a,  0x3,    0x2,    0x2,    0x2,    0x38a,  0x38b,
      0x3,   0x2,   0x2,    0x2,    0x38b,  0x38d,  0x7,    0x8,    0x2,
      0x2,   0x38c, 0x38e,  0x7,    0x71,   0x2,    0x2,    0x38d,  0x38c,
      0x3,   0x2,   0x2,    0x2,    0x38d,  0x38e,  0x3,    0x2,    0x2,
      0x2,   0x38e, 0x38f,  0x3,    0x2,    0x2,    0x2,    0x38f,  0x391,
      0x5,   0x7c,  0x3f,   0x2,    0x390,  0x392,  0x7,    0x71,   0x2,
      0x2,   0x391, 0x390,  0x3,    0x2,    0x2,    0x2,    0x391,  0x392,
      0x3,   0x2,   0x2,    0x2,    0x392,  0x398,  0x3,    0x2,    0x2,
      0x2,   0x393, 0x395,  0x7,    0x71,   0x2,    0x2,    0x394,  0x393,
      0x3,   0x2,   0x2,    0x2,    0x394,  0x395,  0x3,    0x2,    0x2,
      0x2,   0x395, 0x396,  0x3,    0x2,    0x2,    0x2,    0x396,  0x397,
      0x7,   0xe,   0x2,    0x2,    0x397,  0x399,  0x5,    0x56,   0x2c,
      0x2,   0x398, 0x394,  0x3,    0x2,    0x2,    0x2,    0x398,  0x399,
      0x3,   0x2,   0x2,    0x2,    0x399,  0x39a,  0x3,    0x2,    0x2,
      0x2,   0x39a, 0x39b,  0x7,    0x9,    0x2,    0x2,    0x39b,  0x3d9,
      0x3,   0x2,   0x2,    0x2,    0x39c,  0x39e,  0x7,    0x43,   0x2,
      0x2,   0x39d, 0x39f,  0x7,    0x71,   0x2,    0x2,    0x39e,  0x39d,
      0x3,   0x2,   0x2,    0x2,    0x39e,  0x39f,  0x3,    0x2,    0x2,
      0x2,   0x39f, 0x3a0,  0x3,    0x2,    0x2,    0x2,    0x3a0,  0x3a2,
      0x7,   0x8,   0x2,    0x2,    0x3a1,  0x3a3,  0x7,    0x71,   0x2,
      0x2,   0x3a2, 0x3a1,  0x3,    0x2,    0x2,    0x2,    0x3a2,  0x3a3,
      0x3,   0x2,   0x2,    0x2,    0x3a3,  0x3a4,  0x3,    0x2,    0x2,
      0x2,   0x3a4, 0x3a6,  0x5,    0x7c,   0x3f,   0x2,    0x3a5,  0x3a7,
      0x7,   0x71,  0x2,    0x2,    0x3a6,  0x3a5,  0x3,    0x2,    0x2,
      0x2,   0x3a6, 0x3a7,  0x3,    0x2,    0x2,    0x2,    0x3a7,  0x3a8,
      0x3,   0x2,   0x2,    0x2,    0x3a8,  0x3a9,  0x7,    0x9,    0x2,
      0x2,   0x3a9, 0x3d9,  0x3,    0x2,    0x2,    0x2,    0x3aa,  0x3ac,
      0x7,   0x68,  0x2,    0x2,    0x3ab,  0x3ad,  0x7,    0x71,   0x2,
      0x2,   0x3ac, 0x3ab,  0x3,    0x2,    0x2,    0x2,    0x3ac,  0x3ad,
      0x3,   0x2,   0x2,    0x2,    0x3ad,  0x3ae,  0x3,    0x2,    0x2,
      0x2,   0x3ae, 0x3b0,  0x7,    0x8,    0x2,    0x2,    0x3af,  0x3b1,
      0x7,   0x71,  0x2,    0x2,    0x3b0,  0x3af,  0x3,    0x2,    0x2,
      0x2,   0x3b0, 0x3b1,  0x3,    0x2,    0x2,    0x2,    0x3b1,  0x3b2,
      0x3,   0x2,   0x2,    0x2,    0x3b2,  0x3b4,  0x5,    0x7c,   0x3f,
      0x2,   0x3b3, 0x3b5,  0x7,    0x71,   0x2,    0x2,    0x3b4,  0x3b3,
      0x3,   0x2,   0x2,    0x2,    0x3b4,  0x3b5,  0x3,    0x2,    0x2,
      0x2,   0x3b5, 0x3b6,  0x3,    0x2,    0x2,    0x2,    0x3b6,  0x3b7,
      0x7,   0x9,   0x2,    0x2,    0x3b7,  0x3d9,  0x3,    0x2,    0x2,
      0x2,   0x3b8, 0x3ba,  0x7,    0x69,   0x2,    0x2,    0x3b9,  0x3bb,
      0x7,   0x71,  0x2,    0x2,    0x3ba,  0x3b9,  0x3,    0x2,    0x2,
      0x2,   0x3ba, 0x3bb,  0x3,    0x2,    0x2,    0x2,    0x3bb,  0x3bc,
      0x3,   0x2,   0x2,    0x2,    0x3bc,  0x3be,  0x7,    0x8,    0x2,
      0x2,   0x3bd, 0x3bf,  0x7,    0x71,   0x2,    0x2,    0x3be,  0x3bd,
      0x3,   0x2,   0x2,    0x2,    0x3be,  0x3bf,  0x3,    0x2,    0x2,
      0x2,   0x3bf, 0x3c0,  0x3,    0x2,    0x2,    0x2,    0x3c0,  0x3c2,
      0x5,   0x7c,  0x3f,   0x2,    0x3c1,  0x3c3,  0x7,    0x71,   0x2,
      0x2,   0x3c2, 0x3c1,  0x3,    0x2,    0x2,    0x2,    0x3c2,  0x3c3,
      0x3,   0x2,   0x2,    0x2,    0x3c3,  0x3c4,  0x3,    0x2,    0x2,
      0x2,   0x3c4, 0x3c5,  0x7,    0x9,    0x2,    0x2,    0x3c5,  0x3d9,
      0x3,   0x2,   0x2,    0x2,    0x3c6,  0x3c8,  0x7,    0x6a,   0x2,
      0x2,   0x3c7, 0x3c9,  0x7,    0x71,   0x2,    0x2,    0x3c8,  0x3c7,
      0x3,   0x2,   0x2,    0x2,    0x3c8,  0x3c9,  0x3,    0x2,    0x2,
      0x2,   0x3c9, 0x3ca,  0x3,    0x2,    0x2,    0x2,    0x3ca,  0x3cc,
      0x7,   0x8,   0x2,    0x2,    0x3cb,  0x3cd,  0x7,    0x71,   0x2,
      0x2,   0x3cc, 0x3cb,  0x3,    0x2,    0x2,    0x2,    0x3cc,  0x3cd,
      0x3,   0x2,   0x2,    0x2,    0x3cd,  0x3ce,  0x3,    0x2,    0x2,
      0x2,   0x3ce, 0x3d0,  0x5,    0x7c,   0x3f,   0x2,    0x3cf,  0x3d1,
      0x7,   0x71,  0x2,    0x2,    0x3d0,  0x3cf,  0x3,    0x2,    0x2,
      0x2,   0x3d0, 0x3d1,  0x3,    0x2,    0x2,    0x2,    0x3d1,  0x3d2,
      0x3,   0x2,   0x2,    0x2,    0x3d2,  0x3d3,  0x7,    0x9,    0x2,
      0x2,   0x3d3, 0x3d9,  0x3,    0x2,    0x2,    0x2,    0x3d4,  0x3d9,
      0x5,   0x7a,  0x3e,   0x2,    0x3d5,  0x3d9,  0x5,    0x78,   0x3d,
      0x2,   0x3d6, 0x3d9,  0x5,    0x80,   0x41,   0x2,    0x3d7,  0x3d9,
      0x5,   0x88,  0x45,   0x2,    0x3d8,  0x369,  0x3,    0x2,    0x2,
      0x2,   0x3d8, 0x36a,  0x3,    0x2,    0x2,    0x2,    0x3d8,  0x36b,
      0x3,   0x2,   0x2,    0x2,    0x3d8,  0x378,  0x3,    0x2,    0x2,
      0x2,   0x3d8, 0x379,  0x3,    0x2,    0x2,    0x2,    0x3d8,  0x387,
      0x3,   0x2,   0x2,    0x2,    0x3d8,  0x39c,  0x3,    0x2,    0x2,
      0x2,   0x3d8, 0x3aa,  0x3,    0x2,    0x2,    0x2,    0x3d8,  0x3b8,
      0x3,   0x2,   0x2,    0x2,    0x3d8,  0x3c6,  0x3,    0x2,    0x2,
      0x2,   0x3d8, 0x3d4,  0x3,    0x2,    0x2,    0x2,    0x3d8,  0x3d5,
      0x3,   0x2,   0x2,    0x2,    0x3d8,  0x3d6,  0x3,    0x2,    0x2,
      0x2,   0x3d8, 0x3d7,  0x3,    0x2,    0x2,    0x2,    0x3d9,  0x6f,
      0x3,   0x2,   0x2,    0x2,    0x3da,  0x3e1,  0x5,    0x8a,   0x46,
      0x2,   0x3db, 0x3e1,  0x7,    0x34,   0x2,    0x2,    0x3dc,  0x3e1,
      0x5,   0x72,  0x3a,   0x2,    0x3dd,  0x3e1,  0x7,    0x64,   0x2,
      0x2,   0x3de, 0x3e1,  0x5,    0x8c,   0x47,   0x2,    0x3df,  0x3e1,
      0x5,   0x74,  0x3b,   0x2,    0x3e0,  0x3da,  0x3,    0x2,    0x2,
      0x2,   0x3e0, 0x3db,  0x3,    0x2,    0x2,    0x2,    0x3e0,  0x3dc,
      0x3,   0x2,   0x2,    0x2,    0x3e0,  0x3dd,  0x3,    0x2,    0x2,
      0x2,   0x3e0, 0x3de,  0x3,    0x2,    0x2,    0x2,    0x3e0,  0x3df,
      0x3,   0x2,   0x2,    0x2,    0x3e1,  0x71,   0x3,    0x2,    0x2,
      0x2,   0x3e2, 0x3e3,  0x9,    0x4,    0x2,    0x2,    0x3e3,  0x73,
      0x3,   0x2,   0x2,    0x2,    0x3e4,  0x3e6,  0x7,    0xa,    0x2,
      0x2,   0x3e5, 0x3e7,  0x7,    0x71,   0x2,    0x2,    0x3e6,  0x3e5,
      0x3,   0x2,   0x2,    0x2,    0x3e6,  0x3e7,  0x3,    0x2,    0x2,
      0x2,   0x3e7, 0x3f9,  0x3,    0x2,    0x2,    0x2,    0x3e8,  0x3ea,
      0x5,   0x56,  0x2c,   0x2,    0x3e9,  0x3eb,  0x7,    0x71,   0x2,
      0x2,   0x3ea, 0x3e9,  0x3,    0x2,    0x2,    0x2,    0x3ea,  0x3eb,
      0x3,   0x2,   0x2,    0x2,    0x3eb,  0x3f6,  0x3,    0x2,    0x2,
      0x2,   0x3ec, 0x3ee,  0x7,    0x4,    0x2,    0x2,    0x3ed,  0x3ef,
      0x7,   0x71,  0x2,    0x2,    0x3ee,  0x3ed,  0x3,    0x2,    0x2,
      0x2,   0x3ee, 0x3ef,  0x3,    0x2,    0x2,    0x2,    0x3ef,  0x3f0,
      0x3,   0x2,   0x2,    0x2,    0x3f0,  0x3f2,  0x5,    0x56,   0x2c,
      0x2,   0x3f1, 0x3f3,  0x7,    0x71,   0x2,    0x2,    0x3f2,  0x3f1,
      0x3,   0x2,   0x2,    0x2,    0x3f2,  0x3f3,  0x3,    0x2,    0x2,
      0x2,   0x3f3, 0x3f5,  0x3,    0x2,    0x2,    0x2,    0x3f4,  0x3ec,
      0x3,   0x2,   0x2,    0x2,    0x3f5,  0x3f8,  0x3,    0x2,    0x2,
      0x2,   0x3f6, 0x3f4,  0x3,    0x2,    0x2,    0x2,    0x3f6,  0x3f7,
      0x3,   0x2,   0x2,    0x2,    0x3f7,  0x3fa,  0x3,    0x2,    0x2,
      0x2,   0x3f8, 0x3f6,  0x3,    0x2,    0x2,    0x2,    0x3f9,  0x3e8,
      0x3,   0x2,   0x2,    0x2,    0x3f9,  0x3fa,  0x3,    0x2,    0x2,
      0x2,   0x3fa, 0x3fb,  0x3,    0x2,    0x2,    0x2,    0x3fb,  0x3fc,
      0x7,   0xc,   0x2,    0x2,    0x3fc,  0x75,   0x3,    0x2,    0x2,
      0x2,   0x3fd, 0x3ff,  0x7,    0x5,    0x2,    0x2,    0x3fe,  0x400,
      0x7,   0x71,  0x2,    0x2,    0x3ff,  0x3fe,  0x3,    0x2,    0x2,
      0x2,   0x3ff, 0x400,  0x3,    0x2,    0x2,    0x2,    0x400,  0x401,
      0x3,   0x2,   0x2,    0x2,    0x401,  0x421,  0x5,    0x62,   0x32,
      0x2,   0x402, 0x404,  0x7,    0x16,   0x2,    0x2,    0x403,  0x405,
      0x7,   0x71,  0x2,    0x2,    0x404,  0x403,  0x3,    0x2,    0x2,
      0x2,   0x404, 0x405,  0x3,    0x2,    0x2,    0x2,    0x405,  0x406,
      0x3,   0x2,   0x2,    0x2,    0x406,  0x421,  0x5,    0x62,   0x32,
      0x2,   0x407, 0x409,  0x7,    0x17,   0x2,    0x2,    0x408,  0x40a,
      0x7,   0x71,  0x2,    0x2,    0x409,  0x408,  0x3,    0x2,    0x2,
      0x2,   0x409, 0x40a,  0x3,    0x2,    0x2,    0x2,    0x40a,  0x40b,
      0x3,   0x2,   0x2,    0x2,    0x40b,  0x421,  0x5,    0x62,   0x32,
      0x2,   0x40c, 0x40e,  0x7,    0x18,   0x2,    0x2,    0x40d,  0x40f,
      0x7,   0x71,  0x2,    0x2,    0x40e,  0x40d,  0x3,    0x2,    0x2,
      0x2,   0x40e, 0x40f,  0x3,    0x2,    0x2,    0x2,    0x40f,  0x410,
      0x3,   0x2,   0x2,    0x2,    0x410,  0x421,  0x5,    0x62,   0x32,
      0x2,   0x411, 0x413,  0x7,    0x19,   0x2,    0x2,    0x412,  0x414,
      0x7,   0x71,  0x2,    0x2,    0x413,  0x412,  0x3,    0x2,    0x2,
      0x2,   0x413, 0x414,  0x3,    0x2,    0x2,    0x2,    0x414,  0x415,
      0x3,   0x2,   0x2,    0x2,    0x415,  0x421,  0x5,    0x62,   0x32,
      0x2,   0x416, 0x418,  0x7,    0x1a,   0x2,    0x2,    0x417,  0x419,
      0x7,   0x71,  0x2,    0x2,    0x418,  0x417,  0x3,    0x2,    0x2,
      0x2,   0x418, 0x419,  0x3,    0x2,    0x2,    0x2,    0x419,  0x41a,
      0x3,   0x2,   0x2,    0x2,    0x41a,  0x421,  0x5,    0x62,   0x32,
      0x2,   0x41b, 0x41d,  0x7,    0x1b,   0x2,    0x2,    0x41c,  0x41e,
      0x7,   0x71,  0x2,    0x2,    0x41d,  0x41c,  0x3,    0x2,    0x2,
      0x2,   0x41d, 0x41e,  0x3,    0x2,    0x2,    0x2,    0x41e,  0x41f,
      0x3,   0x2,   0x2,    0x2,    0x41f,  0x421,  0x5,    0x62,   0x32,
      0x2,   0x420, 0x3fd,  0x3,    0x2,    0x2,    0x2,    0x420,  0x402,
      0x3,   0x2,   0x2,    0x2,    0x420,  0x407,  0x3,    0x2,    0x2,
      0x2,   0x420, 0x40c,  0x3,    0x2,    0x2,    0x2,    0x420,  0x411,
      0x3,   0x2,   0x2,    0x2,    0x420,  0x416,  0x3,    0x2,    0x2,
      0x2,   0x420, 0x41b,  0x3,    0x2,    0x2,    0x2,    0x421,  0x77,
      0x3,   0x2,   0x2,    0x2,    0x422,  0x424,  0x7,    0x8,    0x2,
      0x2,   0x423, 0x425,  0x7,    0x71,   0x2,    0x2,    0x424,  0x423,
      0x3,   0x2,   0x2,    0x2,    0x424,  0x425,  0x3,    0x2,    0x2,
      0x2,   0x425, 0x426,  0x3,    0x2,    0x2,    0x2,    0x426,  0x428,
      0x5,   0x56,  0x2c,   0x2,    0x427,  0x429,  0x7,    0x71,   0x2,
      0x2,   0x428, 0x427,  0x3,    0x2,    0x2,    0x2,    0x428,  0x429,
      0x3,   0x2,   0x2,    0x2,    0x429,  0x42a,  0x3,    0x2,    0x2,
      0x2,   0x42a, 0x42b,  0x7,    0x9,    0x2,    0x2,    0x42b,  0x79,
      0x3,   0x2,   0x2,    0x2,    0x42c,  0x431,  0x5,    0x40,   0x21,
      0x2,   0x42d, 0x42f,  0x7,    0x71,   0x2,    0x2,    0x42e,  0x42d,
      0x3,   0x2,   0x2,    0x2,    0x42e,  0x42f,  0x3,    0x2,    0x2,
      0x2,   0x42f, 0x430,  0x3,    0x2,    0x2,    0x2,    0x430,  0x432,
      0x5,   0x42,  0x22,   0x2,    0x431,  0x42e,  0x3,    0x2,    0x2,
      0x2,   0x432, 0x433,  0x3,    0x2,    0x2,    0x2,    0x433,  0x431,
      0x3,   0x2,   0x2,    0x2,    0x433,  0x434,  0x3,    0x2,    0x2,
      0x2,   0x434, 0x7b,   0x3,    0x2,    0x2,    0x2,    0x435,  0x43a,
      0x5,   0x7e,  0x40,   0x2,    0x436,  0x438,  0x7,    0x71,   0x2,
      0x2,   0x437, 0x436,  0x3,    0x2,    0x2,    0x2,    0x437,  0x438,
      0x3,   0x2,   0x2,    0x2,    0x438,  0x439,  0x3,    0x2,    0x2,
      0x2,   0x439, 0x43b,  0x5,    0x36,   0x1c,   0x2,    0x43a,  0x437,
      0x3,   0x2,   0x2,    0x2,    0x43a,  0x43b,  0x3,    0x2,    0x2,
      0x2,   0x43b, 0x7d,   0x3,    0x2,    0x2,    0x2,    0x43c,  0x43d,
      0x5,   0x88,  0x45,   0x2,    0x43d,  0x43e,  0x7,    0x71,   0x2,
      0x2,   0x43e, 0x43f,  0x7,    0x5f,   0x2,    0x2,    0x43f,  0x440,
      0x7,   0x71,  0x2,    0x2,    0x440,  0x441,  0x5,    0x56,   0x2c,
      0x2,   0x441, 0x7f,   0x3,    0x2,    0x2,    0x2,    0x442,  0x444,
      0x5,   0x82,  0x42,   0x2,    0x443,  0x445,  0x7,    0x71,   0x2,
      0x2,   0x444, 0x443,  0x3,    0x2,    0x2,    0x2,    0x444,  0x445,
      0x3,   0x2,   0x2,    0x2,    0x445,  0x446,  0x3,    0x2,    0x2,
      0x2,   0x446, 0x448,  0x7,    0x8,    0x2,    0x2,    0x447,  0x449,
      0x7,   0x71,  0x2,    0x2,    0x448,  0x447,  0x3,    0x2,    0x2,
      0x2,   0x448, 0x449,  0x3,    0x2,    0x2,    0x2,    0x449,  0x44e,
      0x3,   0x2,   0x2,    0x2,    0x44a,  0x44c,  0x7,    0x50,   0x2,
      0x2,   0x44b, 0x44d,  0x7,    0x71,   0x2,    0x2,    0x44c,  0x44b,
      0x3,   0x2,   0x2,    0x2,    0x44c,  0x44d,  0x3,    0x2,    0x2,
      0x2,   0x44d, 0x44f,  0x3,    0x2,    0x2,    0x2,    0x44e,  0x44a,
      0x3,   0x2,   0x2,    0x2,    0x44e,  0x44f,  0x3,    0x2,    0x2,
      0x2,   0x44f, 0x461,  0x3,    0x2,    0x2,    0x2,    0x450,  0x452,
      0x5,   0x56,  0x2c,   0x2,    0x451,  0x453,  0x7,    0x71,   0x2,
      0x2,   0x452, 0x451,  0x3,    0x2,    0x2,    0x2,    0x452,  0x453,
      0x3,   0x2,   0x2,    0x2,    0x453,  0x45e,  0x3,    0x2,    0x2,
      0x2,   0x454, 0x456,  0x7,    0x4,    0x2,    0x2,    0x455,  0x457,
      0x7,   0x71,  0x2,    0x2,    0x456,  0x455,  0x3,    0x2,    0x2,
      0x2,   0x456, 0x457,  0x3,    0x2,    0x2,    0x2,    0x457,  0x458,
      0x3,   0x2,   0x2,    0x2,    0x458,  0x45a,  0x5,    0x56,   0x2c,
      0x2,   0x459, 0x45b,  0x7,    0x71,   0x2,    0x2,    0x45a,  0x459,
      0x3,   0x2,   0x2,    0x2,    0x45a,  0x45b,  0x3,    0x2,    0x2,
      0x2,   0x45b, 0x45d,  0x3,    0x2,    0x2,    0x2,    0x45c,  0x454,
      0x3,   0x2,   0x2,    0x2,    0x45d,  0x460,  0x3,    0x2,    0x2,
      0x2,   0x45e, 0x45c,  0x3,    0x2,    0x2,    0x2,    0x45e,  0x45f,
      0x3,   0x2,   0x2,    0x2,    0x45f,  0x462,  0x3,    0x2,    0x2,
      0x2,   0x460, 0x45e,  0x3,    0x2,    0x2,    0x2,    0x461,  0x450,
      0x3,   0x2,   0x2,    0x2,    0x461,  0x462,  0x3,    0x2,    0x2,
      0x2,   0x462, 0x463,  0x3,    0x2,    0x2,    0x2,    0x463,  0x464,
      0x7,   0x9,   0x2,    0x2,    0x464,  0x81,   0x3,    0x2,    0x2,
      0x2,   0x465, 0x466,  0x9,    0x5,    0x2,    0x2,    0x466,  0x83,
      0x3,   0x2,   0x2,    0x2,    0x467,  0x469,  0x7,    0xa,    0x2,
      0x2,   0x468, 0x46a,  0x7,    0x71,   0x2,    0x2,    0x469,  0x468,
      0x3,   0x2,   0x2,    0x2,    0x469,  0x46a,  0x3,    0x2,    0x2,
      0x2,   0x46a, 0x46b,  0x3,    0x2,    0x2,    0x2,    0x46b,  0x474,
      0x5,   0x7c,  0x3f,   0x2,    0x46c,  0x46e,  0x7,    0x71,   0x2,
      0x2,   0x46d, 0x46c,  0x3,    0x2,    0x2,    0x2,    0x46d,  0x46e,
      0x3,   0x2,   0x2,    0x2,    0x46e,  0x46f,  0x3,    0x2,    0x2,
      0x2,   0x46f, 0x471,  0x7,    0xe,    0x2,    0x2,    0x470,  0x472,
      0x7,   0x71,  0x2,    0x2,    0x471,  0x470,  0x3,    0x2,    0x2,
      0x2,   0x471, 0x472,  0x3,    0x2,    0x2,    0x2,    0x472,  0x473,
      0x3,   0x2,   0x2,    0x2,    0x473,  0x475,  0x5,    0x56,   0x2c,
      0x2,   0x474, 0x46d,  0x3,    0x2,    0x2,    0x2,    0x474,  0x475,
      0x3,   0x2,   0x2,    0x2,    0x475,  0x477,  0x3,    0x2,    0x2,
      0x2,   0x476, 0x478,  0x7,    0x71,   0x2,    0x2,    0x477,  0x476,
      0x3,   0x2,   0x2,    0x2,    0x477,  0x478,  0x3,    0x2,    0x2,
      0x2,   0x478, 0x479,  0x3,    0x2,    0x2,    0x2,    0x479,  0x47a,
      0x7,   0xc,   0x2,    0x2,    0x47a,  0x85,   0x3,    0x2,    0x2,
      0x2,   0x47b, 0x47d,  0x7,    0x71,   0x2,    0x2,    0x47c,  0x47b,
      0x3,   0x2,   0x2,    0x2,    0x47c,  0x47d,  0x3,    0x2,    0x2,
      0x2,   0x47d, 0x47e,  0x3,    0x2,    0x2,    0x2,    0x47e,  0x480,
      0x7,   0x1c,  0x2,    0x2,    0x47f,  0x481,  0x7,    0x71,   0x2,
      0x2,   0x480, 0x47f,  0x3,    0x2,    0x2,    0x2,    0x480,  0x481,
      0x3,   0x2,   0x2,    0x2,    0x481,  0x486,  0x3,    0x2,    0x2,
      0x2,   0x482, 0x483,  0x5,    0x92,   0x4a,   0x2,    0x483,  0x484,
      0x9,   0x6,   0x2,    0x2,    0x484,  0x487,  0x3,    0x2,    0x2,
      0x2,   0x485, 0x487,  0x5,    0x92,   0x4a,   0x2,    0x486,  0x482,
      0x3,   0x2,   0x2,    0x2,    0x486,  0x485,  0x3,    0x2,    0x2,
      0x2,   0x487, 0x87,   0x3,    0x2,    0x2,    0x2,    0x488,  0x489,
      0x5,   0x98,  0x4d,   0x2,    0x489,  0x89,   0x3,    0x2,    0x2,
      0x2,   0x48a, 0x48d,  0x5,    0x96,   0x4c,   0x2,    0x48b,  0x48d,
      0x5,   0x94,  0x4b,   0x2,    0x48c,  0x48a,  0x3,    0x2,    0x2,
      0x2,   0x48c, 0x48b,  0x3,    0x2,    0x2,    0x2,    0x48d,  0x8b,
      0x3,   0x2,   0x2,    0x2,    0x48e,  0x490,  0x7,    0x1e,   0x2,
      0x2,   0x48f, 0x491,  0x7,    0x71,   0x2,    0x2,    0x490,  0x48f,
      0x3,   0x2,   0x2,    0x2,    0x490,  0x491,  0x3,    0x2,    0x2,
      0x2,   0x491, 0x4b3,  0x3,    0x2,    0x2,    0x2,    0x492,  0x494,
      0x5,   0x92,  0x4a,   0x2,    0x493,  0x495,  0x7,    0x71,   0x2,
      0x2,   0x494, 0x493,  0x3,    0x2,    0x2,    0x2,    0x494,  0x495,
      0x3,   0x2,   0x2,    0x2,    0x495,  0x496,  0x3,    0x2,    0x2,
      0x2,   0x496, 0x498,  0x7,    0xd,    0x2,    0x2,    0x497,  0x499,
      0x7,   0x71,  0x2,    0x2,    0x498,  0x497,  0x3,    0x2,    0x2,
      0x2,   0x498, 0x499,  0x3,    0x2,    0x2,    0x2,    0x499,  0x49a,
      0x3,   0x2,   0x2,    0x2,    0x49a,  0x49c,  0x5,    0x56,   0x2c,
      0x2,   0x49b, 0x49d,  0x7,    0x71,   0x2,    0x2,    0x49c,  0x49b,
      0x3,   0x2,   0x2,    0x2,    0x49c,  0x49d,  0x3,    0x2,    0x2,
      0x2,   0x49d, 0x4b0,  0x3,    0x2,    0x2,    0x2,    0x49e,  0x4a0,
      0x7,   0x4,   0x2,    0x2,    0x49f,  0x4a1,  0x7,    0x71,   0x2,
      0x2,   0x4a0, 0x49f,  0x3,    0x2,    0x2,    0x2,    0x4a0,  0x4a1,
      0x3,   0x2,   0x2,    0x2,    0x4a1,  0x4a2,  0x3,    0x2,    0x2,
      0x2,   0x4a2, 0x4a4,  0x5,    0x92,   0x4a,   0x2,    0x4a3,  0x4a5,
      0x7,   0x71,  0x2,    0x2,    0x4a4,  0x4a3,  0x3,    0x2,    0x2,
      0x2,   0x4a4, 0x4a5,  0x3,    0x2,    0x2,    0x2,    0x4a5,  0x4a6,
      0x3,   0x2,   0x2,    0x2,    0x4a6,  0x4a8,  0x7,    0xd,    0x2,
      0x2,   0x4a7, 0x4a9,  0x7,    0x71,   0x2,    0x2,    0x4a8,  0x4a7,
      0x3,   0x2,   0x2,    0x2,    0x4a8,  0x4a9,  0x3,    0x2,    0x2,
      0x2,   0x4a9, 0x4aa,  0x3,    0x2,    0x2,    0x2,    0x4aa,  0x4ac,
      0x5,   0x56,  0x2c,   0x2,    0x4ab,  0x4ad,  0x7,    0x71,   0x2,
      0x2,   0x4ac, 0x4ab,  0x3,    0x2,    0x2,    0x2,    0x4ac,  0x4ad,
      0x3,   0x2,   0x2,    0x2,    0x4ad,  0x4af,  0x3,    0x2,    0x2,
      0x2,   0x4ae, 0x49e,  0x3,    0x2,    0x2,    0x2,    0x4af,  0x4b2,
      0x3,   0x2,   0x2,    0x2,    0x4b0,  0x4ae,  0x3,    0x2,    0x2,
      0x2,   0x4b0, 0x4b1,  0x3,    0x2,    0x2,    0x2,    0x4b1,  0x4b4,
      0x3,   0x2,   0x2,    0x2,    0x4b2,  0x4b0,  0x3,    0x2,    0x2,
      0x2,   0x4b3, 0x492,  0x3,    0x2,    0x2,    0x2,    0x4b3,  0x4b4,
      0x3,   0x2,   0x2,    0x2,    0x4b4,  0x4b5,  0x3,    0x2,    0x2,
      0x2,   0x4b5, 0x4b6,  0x7,    0x1f,   0x2,    0x2,    0x4b6,  0x8d,
      0x3,   0x2,   0x2,    0x2,    0x4b7,  0x4ba,  0x7,    0x20,   0x2,
      0x2,   0x4b8, 0x4bb,  0x5,    0x98,   0x4d,   0x2,    0x4b9,  0x4bb,
      0x7,   0x37,  0x2,    0x2,    0x4ba,  0x4b8,  0x3,    0x2,    0x2,
      0x2,   0x4ba, 0x4b9,  0x3,    0x2,    0x2,    0x2,    0x4bb,  0x8f,
      0x3,   0x2,   0x2,    0x2,    0x4bc,  0x4c1,  0x5,    0x6e,   0x38,
      0x2,   0x4bd, 0x4bf,  0x7,    0x71,   0x2,    0x2,    0x4be,  0x4bd,
      0x3,   0x2,   0x2,    0x2,    0x4be,  0x4bf,  0x3,    0x2,    0x2,
      0x2,   0x4bf, 0x4c0,  0x3,    0x2,    0x2,    0x2,    0x4c0,  0x4c2,
      0x5,   0x86,  0x44,   0x2,    0x4c1,  0x4be,  0x3,    0x2,    0x2,
      0x2,   0x4c2, 0x4c3,  0x3,    0x2,    0x2,    0x2,    0x4c3,  0x4c1,
      0x3,   0x2,   0x2,    0x2,    0x4c3,  0x4c4,  0x3,    0x2,    0x2,
      0x2,   0x4c4, 0x91,   0x3,    0x2,    0x2,    0x2,    0x4c5,  0x4c6,
      0x5,   0x98,  0x4d,   0x2,    0x4c6,  0x93,   0x3,    0x2,    0x2,
      0x2,   0x4c7, 0x4c8,  0x9,    0x7,    0x2,    0x2,    0x4c8,  0x95,
      0x3,   0x2,   0x2,    0x2,    0x4c9,  0x4ca,  0x9,    0x8,    0x2,
      0x2,   0x4ca, 0x97,   0x3,    0x2,    0x2,    0x2,    0x4cb,  0x4cc,
      0x9,   0x9,   0x2,    0x2,    0x4cc,  0x99,   0x3,    0x2,    0x2,
      0x2,   0x4cd, 0x4ce,  0x9,    0xa,    0x2,    0x2,    0x4ce,  0x9b,
      0x3,   0x2,   0x2,    0x2,    0x4cf,  0x4d0,  0x9,    0xb,    0x2,
      0x2,   0x4d0, 0x9d,   0x3,    0x2,    0x2,    0x2,    0x4d1,  0x4d2,
      0x9,   0xc,   0x2,    0x2,    0x4d2,  0x9f,   0x3,    0x2,    0x2,
      0x2,   0xdd,  0xa1,   0xa5,   0xa8,   0xab,   0xb3,   0xb8,   0xbd,
      0xc2,  0xc9,  0xce,   0xd1,   0xdc,   0xe0,   0xe4,   0xe8,   0xeb,
      0xef,  0xf9,  0x100,  0x10d,  0x111,  0x11b,  0x12d,  0x131,  0x135,
      0x139, 0x13d, 0x142,  0x149,  0x14d,  0x152,  0x159,  0x15d,  0x160,
      0x165, 0x168, 0x16c,  0x16f,  0x177,  0x17b,  0x17f,  0x183,  0x187,
      0x18c, 0x191, 0x195,  0x19a,  0x19d,  0x1a6,  0x1af,  0x1b4,  0x1c1,
      0x1c4, 0x1cc, 0x1d0,  0x1d5,  0x1da,  0x1de,  0x1e3,  0x1e9,  0x1ee,
      0x1f5, 0x1f9, 0x1fd,  0x1ff,  0x203,  0x205,  0x209,  0x20b,  0x211,
      0x217, 0x21b, 0x21e,  0x221,  0x225,  0x22b,  0x22f,  0x232,  0x235,
      0x23b, 0x23e, 0x241,  0x245,  0x24b,  0x24e,  0x251,  0x255,  0x259,
      0x25c, 0x25f, 0x262,  0x265,  0x26b,  0x270,  0x274,  0x277,  0x27c,
      0x281, 0x286, 0x28e,  0x292,  0x294,  0x298,  0x29c,  0x29e,  0x2a0,
      0x2af, 0x2b9, 0x2c3,  0x2c8,  0x2cc,  0x2d3,  0x2d8,  0x2dd,  0x2e1,
      0x2e5, 0x2e9, 0x2ec,  0x2ee,  0x2f3,  0x2f7,  0x2fb,  0x2ff,  0x303,
      0x307, 0x30a, 0x30c,  0x311,  0x315,  0x31a,  0x31f,  0x323,  0x32a,
      0x331, 0x335, 0x339,  0x33d,  0x34c,  0x34f,  0x35c,  0x35e,  0x364,
      0x366, 0x36d, 0x371,  0x375,  0x37b,  0x37f,  0x383,  0x389,  0x38d,
      0x391, 0x394, 0x398,  0x39e,  0x3a2,  0x3a6,  0x3ac,  0x3b0,  0x3b4,
      0x3ba, 0x3be, 0x3c2,  0x3c8,  0x3cc,  0x3d0,  0x3d8,  0x3e0,  0x3e6,
      0x3ea, 0x3ee, 0x3f2,  0x3f6,  0x3f9,  0x3ff,  0x404,  0x409,  0x40e,
      0x413, 0x418, 0x41d,  0x420,  0x424,  0x428,  0x42e,  0x433,  0x437,
      0x43a, 0x444, 0x448,  0x44c,  0x44e,  0x452,  0x456,  0x45a,  0x45e,
      0x461, 0x469, 0x46d,  0x471,  0x474,  0x477,  0x47c,  0x480,  0x486,
      0x48c, 0x490, 0x494,  0x498,  0x49c,  0x4a0,  0x4a4,  0x4a8,  0x4ac,
      0x4b0, 0x4b3, 0x4ba,  0x4be,  0x4c3,
  };

  atn::ATNDeserializer deserializer;
  _atn = deserializer.deserialize(_serializedATN);

  size_t count = _atn.getNumberOfDecisions();
  _decisionToDFA.reserve(count);
  for (size_t i = 0; i < count; i++) {
    _decisionToDFA.emplace_back(_atn.getDecisionState(i), i);
  }
}

CypherParser::Initializer CypherParser::_init;
