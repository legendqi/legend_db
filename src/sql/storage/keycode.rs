use std::fmt::Display;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use serde::de::{DeserializeSeed, EnumAccess, IntoDeserializer, SeqAccess, VariantAccess, Visitor};
use crate::utils::custom_error::{LegendDBError, LegendDBResult};

pub fn serializer<T: serde::ser::Serialize>(key: &T) -> LegendDBResult<Vec<u8>> {
    let mut serializer = KeyCodeSerializer {
        output: Vec::new(),
    };
    key.serialize(&mut serializer)?;
    Ok(serializer.output)
}

pub fn deserializer<'a, T: Deserialize<'a>>(input: &'a [u8]) -> LegendDBResult<T> {
    let mut deserializer = KeyCodeDeserializer {
        input,
    };
    T::deserialize(&mut deserializer)
}
pub struct KeyCodeSerializer {
    pub output: Vec<u8>
}

impl<'a> serde::ser::SerializeSeq for &'a mut KeyCodeSerializer {
    type Ok = ();
    type Error = LegendDBError;

    fn serialize_element<T>(&mut self, value: &T) -> LegendDBResult<()>
    where
        T: ?Sized + Serialize
    {
        value.serialize(&mut **self)
    }

    fn end(self) -> LegendDBResult<Self::Ok> {
        Ok(())
    }
}

impl<'a> serde::ser::SerializeTuple for &'a mut KeyCodeSerializer {
    type Ok = ();
    type Error = LegendDBError;

    fn serialize_element<T>(&mut self, value: &T) -> LegendDBResult<()>
    where
        T: ?Sized + Serialize
    {
        value.serialize(&mut **self)
    }

    fn end(self) -> LegendDBResult<Self::Ok> {
        Ok(())
    }
}

impl<'a> serde::ser::SerializeTupleVariant for &'a mut KeyCodeSerializer {
    type Ok = ();
    type Error = LegendDBError;

    fn serialize_field<T>(&mut self, value: &T) -> LegendDBResult<()>
    where
        T: ?Sized + Serialize
    {
        value.serialize(&mut **self)
    }

    fn end(self) -> LegendDBResult<Self::Ok> {
        Ok(())
    }
}

impl<'a> Serializer for &'a mut KeyCodeSerializer{
    type Ok = ();
    type Error = LegendDBError;
    type SerializeSeq = Self;
    type SerializeTuple = Self;
    type SerializeTupleStruct = serde::ser::Impossible<(), LegendDBError>;
    type SerializeTupleVariant = Self;
    type SerializeMap = serde::ser::Impossible<(),  Self::Error>;
    type SerializeStruct = serde::ser::Impossible<(),  Self::Error>;
    type SerializeStructVariant = serde::ser::Impossible<(),  Self::Error>;

    fn serialize_bool(self, v: bool) -> LegendDBResult<Self::Ok> {
        self.output.push(v as u8);
        Ok(())
    }

    fn serialize_i8(self, v: i8) -> LegendDBResult<Self::Ok> {
        todo!()
    }

    fn serialize_i16(self, v: i16) -> LegendDBResult<Self::Ok> {
        todo!()
    }

    fn serialize_i32(self, v: i32) -> LegendDBResult<Self::Ok> {
        todo!()
    }

    fn serialize_i64(self, v: i64) -> LegendDBResult<Self::Ok> {
        Ok(self.output.extend(v.to_be_bytes()))
    }

    fn serialize_i128(self, v: i128) -> LegendDBResult<Self::Ok> {
        todo!()
    }

    fn serialize_u8(self, v: u8) -> LegendDBResult<Self::Ok> {
        todo!()
    }

    fn serialize_u16(self, v: u16) -> LegendDBResult<Self::Ok> {
        todo!()
    }

    fn serialize_u32(self, v: u32) -> LegendDBResult<Self::Ok> {
        todo!()
    }

    fn serialize_u64(self, v: u64) -> LegendDBResult<Self::Ok> {
        self.output.extend(v.to_be_bytes());
        Ok(())
    }

    fn serialize_u128(self, v: u128) -> LegendDBResult<Self::Ok> {
        todo!()
    }

    fn serialize_f32(self, v: f32) -> LegendDBResult<Self::Ok> {
        todo!()
    }

    fn serialize_f64(self, v: f64) -> LegendDBResult<Self::Ok> {
        todo!()
    }

    fn serialize_char(self, v: char) -> LegendDBResult<Self::Ok> {
        todo!()
    }

    fn serialize_str(self, v: &str) -> LegendDBResult<Self::Ok> {
        self.output.extend(v.as_bytes());
        Ok(())
    }

    //原始值           编码后
    //97 98 99        97 98 99 0 0
    //97 98 0 99      97 98 0 255 99 00
    //97 98 0 0 99    97 98 0 255 0 255 99 0 0
    fn serialize_bytes(self, v: &[u8]) -> LegendDBResult<Self::Ok> {
        v.into_iter().for_each(|v| {
            if v == &0 {
                self.output.extend([0, 255]);
            } else {
                self.output.extend([v]);
            }
        });
        // 最后放2个0表示结尾
        self.output.extend([0, 0]);
        Ok(())
    }

    fn serialize_none(self) -> LegendDBResult<Self::Ok> {
        todo!()
    }

    fn serialize_some<T>(self, value: &T) -> LegendDBResult<Self::Ok>
    where
        T: ?Sized + Serialize
    {
        todo!()
    }

    fn serialize_unit(self) -> LegendDBResult<Self::Ok> {
        todo!()
    }

    fn serialize_unit_struct(self, name: &'static str) -> LegendDBResult<Self::Ok> {
        todo!()
    }

    // 类似MvccKey::NextVersion
    fn serialize_unit_variant(self, name: &'static str, variant_index: u32, variant: &'static str) -> LegendDBResult<Self::Ok> {
        self.output.extend(u8::try_from(variant_index));
        Ok(())
    }

    fn serialize_newtype_struct<T>(self, name: &'static str, value: &T) -> LegendDBResult<Self::Ok>
    where
        T: ?Sized + Serialize
    {
        todo!()
    }

    // 类似MvccKey::TxnActive(Version)
    fn serialize_newtype_variant<T>(self, name: &'static str, variant_index: u32, variant: &'static str, value: &T) -> LegendDBResult<Self::Ok>
    where
        T: ?Sized + Serialize
    {
        self.serialize_unit_variant(name, variant_index, variant)?;
        value.serialize(self)
    }

    fn serialize_seq(self, len: Option<usize>) -> LegendDBResult<Self::SerializeSeq> {
        Ok(self)
    }

    fn serialize_tuple(self, len: usize) -> LegendDBResult<Self::SerializeTuple> {
        Ok(self)
    }

    fn serialize_tuple_struct(self, name: &'static str, len: usize) -> LegendDBResult<Self::SerializeTupleStruct> {
        todo!()
    }
    // 类似MvccKey::TxnWrite(Version, Vec<u8>)
    fn serialize_tuple_variant(self, name: &'static str, variant_index: u32, variant: &'static str, len: usize) -> LegendDBResult<Self::SerializeTupleVariant> {
        self.serialize_unit_variant(name, variant_index, variant)?;
        Ok(self)
    }

    fn serialize_map(self, len: Option<usize>) -> LegendDBResult<Self::SerializeMap> {
        todo!()
    }

    fn serialize_struct(self, name: &'static str, len: usize) -> LegendDBResult<Self::SerializeStruct> {
        todo!()
    }

    fn serialize_struct_variant(self, name: &'static str, variant_index: u32, variant: &'static str, len: usize) -> LegendDBResult<Self::SerializeStructVariant> {
        todo!()
    }

    fn collect_seq<I>(self, iter: I) -> LegendDBResult<Self::Ok>
    where
        I: IntoIterator,
        <I as IntoIterator>::Item: Serialize
    {
        todo!()
    }

    fn collect_map<K, V, I>(self, iter: I) -> LegendDBResult<Self::Ok>
    where
        K: Serialize,
        V: Serialize,
        I: IntoIterator<Item=(K, V)>
    {
        todo!()
    }

    fn collect_str<T>(self, value: &T) -> LegendDBResult<Self::Ok>
    where
        T: ?Sized + Display
    {
        todo!()
    }

    fn is_human_readable(&self) -> bool {
        todo!()
    }
}

pub struct KeyCodeDeserializer<'de> {
    input: &'de [u8],
}

impl<'de>  KeyCodeDeserializer<'de> {
    fn take_bytes(&mut self, len: usize) -> &[u8] {
        let bytes = &self.input[..len];
        self.input = &self.input[len..];
        bytes
    }
    // 如果之后是255，说明是原始字符串中的0， 则继续解析
    // 如果这个0之后的值是0， 说明是字符串的结尾
    fn next_bytes(&mut self) -> LegendDBResult<Vec<u8>> {
        let mut res = Vec::new();
        let mut iter = self.input.iter().enumerate();
        let index = loop {
            match iter.next() {
                Some((i, &0)) => match iter.next() {
                    Some((i, 0)) => break i + 1,
                    Some((i, 255)) => {
                        res.push(0);
                    },
                    _ => {return Err(LegendDBError::Internal("unexpected input".into()))}
                },
                Some((i, b)) => { res.push(*b) }
                _ => {return Err(LegendDBError::Internal("unexpected input".into()))}
            }
        };
        self.input = &self.input[index..];
        Ok(res)
    }
}

impl<'de> SeqAccess<'de>  for KeyCodeDeserializer<'de>  {
    type Error = LegendDBError;

    fn next_element_seed<T>(&mut self, seed: T) -> LegendDBResult<Option<T::Value>>
    where
        T: DeserializeSeed<'de>
    {
        seed.deserialize(self).map(Some)
    }
}

impl<'de, 'a> VariantAccess<'de>  for &mut KeyCodeDeserializer<'de>  {
    type Error = LegendDBError;

    fn unit_variant(self) -> LegendDBResult<()> {
        Ok(())
    }

    fn newtype_variant_seed<T>(self, seed: T) -> LegendDBResult<T::Value>
    where
        T: DeserializeSeed<'de>
    {
        seed.deserialize(self)
    }

    fn tuple_variant<V>(self, len: usize, visitor: V) -> LegendDBResult<V::Value>
    where
        V: Visitor<'de>
    {
        visitor.visit_seq(self)
    }

    fn struct_variant<V>(self, fields: &'static [&'static str], visitor: V) -> LegendDBResult<V::Value>
    where
        V: Visitor<'de>
    {
        todo!()
    }
}

impl<'de, 'a> EnumAccess<'de>  for &mut KeyCodeDeserializer<'de>  {
    type Error = LegendDBError;
    type Variant = Self;

    fn variant_seed<V>(mut self, seed: V) -> LegendDBResult<(V::Value, Self::Variant)>
    where
        V: DeserializeSeed<'de>
    {
        let index = self.take_bytes(1)[0] as u32;
        let varint_index: LegendDBResult<_> = seed.deserialize(index.into_deserializer());
        Ok((varint_index?, self))
    }
}

impl<'de, 'a> Deserializer<'de> for & mut KeyCodeDeserializer<'de>  {
    type Error = LegendDBError;

    fn deserialize_any<V>(self, visitor: V) -> LegendDBResult<V::Value>
    where
        V: Visitor<'de>
    {
        todo!()
    }

    fn deserialize_bool<V>(self, visitor: V) -> LegendDBResult<V::Value>
    where
        V: Visitor<'de>
    {
        let v = self.take_bytes(1)[0];
        // v == 0 ==> false
        // 否则为true
        visitor.visit_bool(v != 0)
    }

    fn deserialize_i8<V>(self, visitor: V) -> LegendDBResult<V::Value>
    where
        V: Visitor<'de>
    {
        todo!()
    }

    fn deserialize_i16<V>(self, visitor: V) -> LegendDBResult<V::Value>
    where
        V: Visitor<'de>
    {
        todo!()
    }

    fn deserialize_i32<V>(self, visitor: V) -> LegendDBResult<V::Value>
    where
        V: Visitor<'de>
    {
        todo!()
    }

    fn deserialize_i64<V>(self, visitor: V) -> LegendDBResult<V::Value>
    where
        V: Visitor<'de>
    {
        let bytes = self.take_bytes(8);
        let v = i64::from_be_bytes(bytes.try_into()?);
        visitor.visit_i64(v)
    }

    fn deserialize_i128<V>(self, visitor: V) -> LegendDBResult<V::Value>
    where
        V: Visitor<'de>
    {
        todo!()
    }

    fn deserialize_u8<V>(self, visitor: V) -> LegendDBResult<V::Value>
    where
        V: Visitor<'de>
    {
        todo!()
    }

    fn deserialize_u16<V>(self, visitor: V) -> LegendDBResult<V::Value>
    where
        V: Visitor<'de>
    {
        todo!()
    }

    fn deserialize_u32<V>(self, visitor: V) -> LegendDBResult<V::Value>
    where
        V: Visitor<'de>
    {
        todo!()
    }

    // &[u8] -> Vec<u8>
    // From  TryFrom
    fn deserialize_u64<V>(mut self, visitor: V) -> LegendDBResult<V::Value>
    where
        V: Visitor<'de>
    {
        let bytes = self.take_bytes(8);
        let value = u64::from_be_bytes(bytes.try_into()?);
        visitor.visit_u64(value)
    }

    fn deserialize_u128<V>(self, visitor: V) -> LegendDBResult<V::Value>
    where
        V: Visitor<'de>
    {
        todo!()
    }

    fn deserialize_f32<V>(self, visitor: V) -> LegendDBResult<V::Value>
    where
        V: Visitor<'de>
    {
        todo!()
    }

    fn deserialize_f64<V>(self, visitor: V) -> LegendDBResult<V::Value>
    where
        V: Visitor<'de>
    {
        todo!()
    }

    fn deserialize_char<V>(self, visitor: V) -> LegendDBResult<V::Value>
    where
        V: Visitor<'de>
    {
        todo!()
    }

    fn deserialize_str<V>(self, visitor: V) -> LegendDBResult<V::Value>
    where
        V: Visitor<'de>
    {
        let bytes = self.next_bytes()?;
        visitor.visit_str(&String::from_utf8(bytes)?)
    }

    fn deserialize_string<V>(self, visitor: V) -> LegendDBResult<V::Value>
    where
        V: Visitor<'de>
    {
        todo!()
    }

    fn deserialize_bytes<V>(mut self, visitor: V) -> LegendDBResult<V::Value>
    where
        V: Visitor<'de>
    {
        visitor.visit_bytes(&self.next_bytes()?)
    }

    fn deserialize_byte_buf<V>(mut self, visitor: V) -> LegendDBResult<V::Value>
    where
        V: Visitor<'de>
    {
        visitor.visit_byte_buf(self.next_bytes()?)
    }

    fn deserialize_option<V>(self, visitor: V) -> LegendDBResult<V::Value>
    where
        V: Visitor<'de>
    {
        todo!()
    }

    fn deserialize_unit<V>(self, visitor: V) -> LegendDBResult<V::Value>
    where
        V: Visitor<'de>
    {
        todo!()
    }

    fn deserialize_unit_struct<V>(self, name: &'static str, visitor: V) -> LegendDBResult<V::Value>
    where
        V: Visitor<'de>
    {
        todo!()
    }

    fn deserialize_newtype_struct<V>(self, name: &'static str, visitor: V) -> LegendDBResult<V::Value>
    where
        V: Visitor<'de>
    {
        todo!()
    }

    fn deserialize_seq<V>(self, visitor: V) -> LegendDBResult<V::Value>
    where
        V: Visitor<'de>
    {
        visitor.visit_seq(self)
    }

    fn deserialize_tuple<V>(self, len: usize, visitor: V) -> LegendDBResult<V::Value>
    where
        V: Visitor<'de>
    {
        visitor.visit_seq(self)
    }

    fn deserialize_tuple_struct<V>(self, name: &'static str, len: usize, visitor: V) -> LegendDBResult<V::Value>
    where
        V: Visitor<'de>
    {
        todo!()
    }

    fn deserialize_map<V>(self, visitor: V) -> LegendDBResult<V::Value>
    where
        V: Visitor<'de>
    {
        todo!()
    }

    fn deserialize_struct<V>(self, name: &'static str, fields: &'static [&'static str], visitor: V) -> LegendDBResult<V::Value>
    where
        V: Visitor<'de>
    {
        todo!()
    }

    fn deserialize_enum<V>(self, name: &'static str, variants: &'static [&'static str], visitor: V) -> LegendDBResult<V::Value>
    where
        V: Visitor<'de>
    {
        visitor.visit_enum(self)
    }

    fn deserialize_identifier<V>(self, visitor: V) -> LegendDBResult<V::Value>
    where
        V: Visitor<'de>
    {
        todo!()
    }

    fn deserialize_ignored_any<V>(self, visitor: V) -> LegendDBResult<V::Value>
    where
        V: Visitor<'de>
    {
        todo!()
    }
}

#[cfg(test)]
mod tests {
    use crate::sql::storage::keycode::serializer;
    use crate::sql::storage::mvcc::MvccKey;

    #[test]
    fn test_encode() {
        let ser_cmp = |k: MvccKey, vec: Vec<u8>| {
            let res = serializer(&k).unwrap();
            assert_eq!(res, vec)
        };
        let k = MvccKey::NextVersion;
        let v = serializer(&k).unwrap();
        println!("{:?}", v);
        ser_cmp(k, v);

        let k = MvccKey::Version(b"abc".to_vec(), 11);
        let v = serializer(&k).unwrap();
        println!("{:?}", v);
        ser_cmp(k, v);
    }
}