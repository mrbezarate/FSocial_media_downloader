use teloxide::types::{InputMedia, InputMediaAudio, InputFile};

fn main() {
    let _ = InputMedia::Audio(InputMediaAudio::new(InputFile::file("foo.mp3")).title("test").performer("test"));
}
