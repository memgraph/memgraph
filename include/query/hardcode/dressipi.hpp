#pragma once

#include "includes.hpp"

namespace hardcode
{

// TODO: decouple hashes from the code because hashes could and should be
// tested separately

auto load_dressipi_functions(Db &db)
{
    query_functions_t functions;

    // Query: CREATE (n {garment_id: 1234, garment_category_id: 1}) RETURN n
    // Hash: 12139093029838549530
    functions[12139093029838549530u] = [&db](properties_t &&args) {
        DbAccessor t(db);
        return t.commit();
    };

    // Query: CREATE (p:profile {profile_id: 111, partner_id: 55}) RETURN p
    // Hash: 17158428452166262783
    functions[17158428452166262783u] = [&db](properties_t &&args) {
        DbAccessor t(db);
        return t.commit();
    };

    // Query: MATCH (g:garment {garment_id: 1234}) SET g:FF RETURN labels(g)
    // Hash: 11123780635391515946
    functions[11123780635391515946u] = [&db](properties_t &&args) {
        DbAccessor t(db);
        return t.commit();
    };
 
    // Query: MATCH (p:profile {profile_id: 111, partner_id: 55})-[s:score]-(g:garment{garment_id: 1234}) SET s.score = 1550 RETURN s.score
    // Hash: 674581607834128909
    functions[674581607834128909u] = [&db](properties_t &&args) {
        DbAccessor t(db);
        return t.commit();
    };
  
    // Query: MATCH (g:garment {garment_id: 3456}) SET g.reveals = 50 RETURN g
    // Hash: 2839969099736071844
    functions[2839969099736071844u] = [&db](properties_t &&args) {
        DbAccessor t(db);
        return t.commit();
    };
  
    // Query: MERGE (g1:garment {garment_id: 1234})-[r:default_outfit]-(g2:garment {garment_id: 2345}) RETURN r
    // Hash: 3782642357973971504
    functions[3782642357973971504u] = [&db](properties_t &&args) {
        DbAccessor t(db);
        return t.commit();
    };
  
    // Query: MERGE (p:profile {profile_id: 111, partner_id: 55})-[s:score]-(g.garment {garment_id: 1234}) SET s.score=1500 RETURN s
    // Hash: 7871009397157280694
    functions[7871009397157280694u] = [&db](properties_t &&args) {
        DbAccessor t(db);
        return t.commit();
    };
  
    // Query: MATCH (p:profile {profile_id: 111, partner_id: 55})-[s:score]-(g.garment {garment_id: 1234}) DELETE s
    // Hash: 9459600951073026137
    functions[9459600951073026137u] = [&db](properties_t &&args) {
        DbAccessor t(db);
        return t.commit();
    };
  
    // Query: MATCH (p:profile {profile_id: 113}) DELETE p
    // Hash: 6763665709953344106
    functions[6763665709953344106u] = [&db](properties_t &&args) {
        DbAccessor t(db);
        return t.commit();
    };
  
    // Query: MATCH (n) DETACH DELETE n
    // Hash: 4798158026600988079
    functions[4798158026600988079u] = [&db](properties_t &&args) {
        DbAccessor t(db);
        return t.commit();
    };
  
    // Query: MATCH (p:profile) RETURN p
    // Hash: 15599970964909894866
    functions[15599970964909894866u] = [&db](properties_t &&args) {
        DbAccessor t(db);
        return t.commit();
    };
 
    // Query: MATCH (g:garment) RETURN COUNT(g)
    // Hash: 11458306387621940265
    functions[11458306387621940265u] = [&db](properties_t &&args) {
        DbAccessor t(db);
        return t.commit();
    };
  
    // Query: MATCH (g:garment {garment_id: 1234}) RETURN g
    // Hash: 7756609649964321221
    functions[7756609649964321221u] = [&db](properties_t &&args) {
        DbAccessor t(db);
        return t.commit();
    };
  
    // Query: MATCH (p:profile {partner_id: 55}) RETURN p
    // Hash: 17506488413143988006
    functions[17506488413143988006u] = [&db](properties_t &&args) {
        DbAccessor t(db);
        return t.commit();
    };
  
    // Query: MATCH (n) RETURN count(n)
    // Hash: 10510787599699014973
    functions[10510787599699014973u] = [&db](properties_t &&args) {
        DbAccessor t(db);
        return t.commit();
    };

    return functions;
}

}
