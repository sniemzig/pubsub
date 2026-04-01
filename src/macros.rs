pub struct EventType<Topic, Payload> {
	pub topic: Topic,
	pub payload: Payload,
}

impl<Topic, Payload> EventType<Topic, Payload> {
	pub fn new(topic: Topic, payload: Payload) -> Self {
		Self { topic, payload }
	}
}

#[macro_export]
macro_rules! event {
	($name:ident => $payload:ty $(,)?) => {
		#[derive(Hash)]
		struct $name;

		impl $crate::Topic for $name {
			type Payload = $payload;
		}
	};
	($name:ident { $($field:ident : $field_ty:ty),* $(,)? } => $payload:ty $(,)?) => {
		#[derive(Hash)]
		struct $name {
			$($field: $field_ty),*
		}

		impl $crate::Topic for $name {
			type Payload = $payload;
		}
	};
}

#[macro_export]
macro_rules! events {
	($topics:ident -> $events:ident, { $($entries:tt)* } $(,)?) => {
		$crate::__pubsub_events! {
			@parse
			[$topics]
			[$events]
			[]
			[]
			[]
			[]
			$($entries)*
		}
	};
	($topics:ident -> $events:ident, $($entries:tt)*) => {
		$crate::__pubsub_events! {
			@parse
			[$topics]
			[$events]
			[]
			[]
			[]
			[]
			$($entries)*
		}
	};
}

#[doc(hidden)]
#[macro_export]
macro_rules! __pubsub_events {
	(@parse
		[$topics:ident]
		[$events:ident]
		[$($decls:tt)*]
		[$($topic_variants:tt)*]
		[$($event_variants:tt)*]
		[$($impls:tt)*]
	) => {
		$($decls)*

		enum $topics {
			$($topic_variants)*
		}

		enum $events {
			$($event_variants)*
		}

		$($impls)*
	};

	(@parse
		[$topics:ident]
		[$events:ident]
		[$($decls:tt)*]
		[$($topic_variants:tt)*]
		[$($event_variants:tt)*]
		[$($impls:tt)*]
		$name:ident => $payload:ty,
		$($rest:tt)*
	) => {
		$crate::__pubsub_events! {
			@parse
			[$topics]
			[$events]
			[
				$($decls)*
				$crate::event!($name => $payload);
			]
			[
				$($topic_variants)*
				$name($name),
			]
			[
				$($event_variants)*
				$name($crate::EventType<$name, $payload>),
			]
			[
				$($impls)*
				impl $crate::IntoEvent<$events> for $name {
					fn into_event(self, payload: $payload) -> $events {
						$events::$name($crate::EventType::new(self, payload))
					}
				}
			]
			$($rest)*
		}
	};
	(@parse
		[$topics:ident]
		[$events:ident]
		[$($decls:tt)*]
		[$($topic_variants:tt)*]
		[$($event_variants:tt)*]
		[$($impls:tt)*]
		$name:ident => $payload:ty
	) => {
		$crate::__pubsub_events! {
			@parse
			[$topics]
			[$events]
			[$($decls)*]
			[$($topic_variants)*]
			[$($event_variants)*]
			[$($impls)*]
			$name => $payload,
		}
	};

	(@parse
		[$topics:ident]
		[$events:ident]
		[$($decls:tt)*]
		[$($topic_variants:tt)*]
		[$($event_variants:tt)*]
		[$($impls:tt)*]
		$name:ident { $($field:ident : $field_ty:ty),* $(,)? } => $payload:ty,
		$($rest:tt)*
	) => {
		$crate::__pubsub_events! {
			@parse
			[$topics]
			[$events]
			[
				$($decls)*
				$crate::event!($name { $($field : $field_ty),* } => $payload);
			]
			[
				$($topic_variants)*
				$name($name),
			]
			[
				$($event_variants)*
				$name($crate::EventType<$name, $payload>),
			]
			[
				$($impls)*
				impl $crate::IntoEvent<$events> for $name {
					fn into_event(self, payload: $payload) -> $events {
						$events::$name($crate::EventType::new(self, payload))
					}
				}
			]
			$($rest)*
		}
	};
	(@parse
		[$topics:ident]
		[$events:ident]
		[$($decls:tt)*]
		[$($topic_variants:tt)*]
		[$($event_variants:tt)*]
		[$($impls:tt)*]
		$name:ident { $($field:ident : $field_ty:ty),* $(,)? } => $payload:ty
	) => {
		$crate::__pubsub_events! {
			@parse
			[$topics]
			[$events]
			[$($decls)*]
			[$($topic_variants)*]
			[$($event_variants)*]
			[$($impls)*]
			$name { $($field : $field_ty),* } => $payload,
		}
	};

	(@parse
		[$topics:ident]
		[$events:ident]
		[$($decls:tt)*]
		[$($topic_variants:tt)*]
		[$($event_variants:tt)*]
		[$($impls:tt)*]
		$name:ident,
		$($rest:tt)*
	) => {
		$crate::__pubsub_events! {
			@parse
			[$topics]
			[$events]
			[$($decls)*]
			[
				$($topic_variants)*
				$name($name),
			]
			[
				$($event_variants)*
				$name($crate::EventType<$name, <$name as $crate::Topic>::Payload>),
			]
			[
				$($impls)*
				impl $crate::IntoEvent<$events> for $name {
					fn into_event(self, payload: <$name as $crate::Topic>::Payload) -> $events {
						$events::$name($crate::EventType::new(self, payload))
					}
				}
			]
			$($rest)*
		}
	};
	(@parse
		[$topics:ident]
		[$events:ident]
		[$($decls:tt)*]
		[$($topic_variants:tt)*]
		[$($event_variants:tt)*]
		[$($impls:tt)*]
		$name:ident
	) => {
		$crate::__pubsub_events! {
			@parse
			[$topics]
			[$events]
			[$($decls)*]
			[$($topic_variants)*]
			[$($event_variants)*]
			[$($impls)*]
			$name,
		}
	};
}
